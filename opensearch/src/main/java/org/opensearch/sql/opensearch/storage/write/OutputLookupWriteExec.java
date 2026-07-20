/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Lifecycle for {@code outputlookup} on substrate C3: every lookup is a slice tagged {@code
 * __lookup=<uuid>} in the single shared {@code .lookups} index behind a filtered alias {@code
 * <name>}, the same artifact the Dashboards data importer produces. The index owns its mapping (see
 * {@link LookupsIndex}), so the write honors it and never infers a keyword-only mapping.
 *
 * <p><b>Consistency, weak and eventual.</b> Overwrite is content-atomic and gap-free because the
 * alias repoint is a single cluster-state update, but the overall contract stays weak because a
 * crash between the slice write and the repoint can orphan a slice or leave the alias unswapped.
 * Append is at-least-once. A keyed write is idempotent within a slice because the deterministic id
 * is salted with the slice uuid.
 *
 * <p><b>Permissions.</b> Runs under the caller security context and needs {@code
 * indices:data/write/bulk} and {@code indices:admin/aliases} on the destination, plus {@code
 * indices:admin/create} the first time {@code .lookups} is created and get plus aliases-get to
 * probe the name. No cluster-level permission.
 */
public final class OutputLookupWriteExec {

  private static final Logger LOGGER = LogManager.getLogger(OutputLookupWriteExec.class);

  private static final int BATCH_SIZE = 1000;

  /** Extracts the {@code __lookup} discriminant value from a term-filter JSON string. */
  private static final Pattern LOOKUP_UUID =
      Pattern.compile("\"" + LookupsIndex.LOOKUP_FIELD + "\"\\s*:\\s*\"([^\"]+)\"");

  private OutputLookupWriteExec() {}

  /**
   * Drain the input, materialize it into a {@code .lookups} slice, publish it through the {@code
   * <name>} filtered alias, and return the number of rows written.
   */
  public static long execute(
      NodeClient client,
      String name,
      List<String> fields,
      WriteMode mode,
      List<String> keyFields,
      @Nullable Integer max,
      int maxRows,
      boolean overrideIfEmpty,
      boolean append,
      Enumerator<@Nullable Object> input) {

    List<Object[]> rows = drain(input, max, maxRows);

    if (!append && rows.isEmpty() && !overrideIfEmpty) {
      return 0;
    }

    LookupsIndex.ensureExists(client);

    Target target = resolveTarget(client, name);
    switch (target.kind()) {
      case ABSENT:
        {
          String uuid = newUuid();
          writeSlice(client, LookupsIndex.INDEX_NAME, fields, mode, keyFields, rows, uuid);
          addFilteredAlias(client, name, LookupsIndex.INDEX_NAME, uuid);
          break;
        }
      case ALIAS:
        {
          if (!target.filtered()) {
            throw new IllegalArgumentException(
                "outputlookup destination ["
                    + name
                    + "] is a non-filtered alias, not a lookup; refusing to write it");
          }
          if (append) {
            if (target.lookupUuid() == null) {
              throw new IllegalArgumentException(
                  "outputlookup cannot append to ["
                      + name
                      + "]: its alias filter has no __lookup discriminant; overwrite it first");
            }
            writeSlice(
                client, target.primaryIndex(), fields, mode, keyFields, rows, target.lookupUuid());
          } else {
            String uuid = newUuid();
            writeSlice(client, LookupsIndex.INDEX_NAME, fields, mode, keyFields, rows, uuid);
            repointFilteredAlias(
                client, name, target.aliasIndices(), LookupsIndex.INDEX_NAME, uuid);
            // TODO(reaper, separate PR): the atomic repoint leaves the previous slice as an
            // orphan (a __lookup uuid in .lookups referenced by no alias). Crash-before-repoint
            // and concurrent same-name overwrite produce the same orphan shape. A reaper reclaims
            // them: enumerate distinct __lookup uuids in .lookups, subtract the set referenced by
            // any filtered alias, delete_by_query the remainder. This info log is the reaper's
            // observability seam until then.
            LOGGER.info(
                "outputlookup overwrite of [{}] wrote a new slice into [{}] and repointed the"
                    + " alias; the previous slice(s) on {} are orphaned pending the reaper",
                name,
                LookupsIndex.INDEX_NAME,
                target.aliasIndices());
          }
          break;
        }
      case INDEX:
        throw new IllegalArgumentException(
            "outputlookup destination ["
                + name
                + "] is a concrete index, not a lookup; delete it first to reuse the name");
      default:
        throw new IllegalStateException("unreachable target kind: " + target.kind());
    }
    return rows.size();
  }

  /**
   * Drain the input rows. {@code max} is the optional per-query {@code max=} limit and truncates
   * silently (the user opted into at-most-N). {@code maxRows} is the operator ceiling {@code
   * plugins.ppl.outputlookup.max_rows}: exceeding it throws (a fail-loud 400) rather than
   * truncating, so an operator guard never silently drops data, and no slice is written.
   */
  private static List<Object[]> drain(
      Enumerator<@Nullable Object> input, @Nullable Integer max, int maxRows) {
    List<Object[]> rows = new ArrayList<>();
    while (input.moveNext()) {
      Object cur = input.current();
      rows.add(cur instanceof Object[] arr ? arr : new Object[] {cur});
      if (max != null && rows.size() >= max) {
        break;
      }
      if (rows.size() > maxRows) {
        throw new IllegalArgumentException(
            "outputlookup wrote nothing because the result has more than "
                + maxRows
                + " rows, the maximum allowed for a single write. To write fewer rows, add"
                + " max=<n> to your query. To allow more, raise the"
                + " plugins.ppl.outputlookup.max_rows setting.");
      }
    }
    return rows;
  }

  /**
   * Bulk the rows into {@code index} as a slice tagged {@code __lookup=<uuid>}. The discriminant is
   * appended to the document fields, and to the key fields on an upsert so the deterministic id is
   * salted with the slice uuid and keyed rows never collide across slices sharing the index.
   */
  private static void writeSlice(
      NodeClient client,
      String index,
      List<String> fields,
      WriteMode mode,
      List<String> keyFields,
      List<Object[]> rows,
      String uuid) {
    List<String> sliceFields = new ArrayList<>(fields);
    sliceFields.add(LookupsIndex.LOOKUP_FIELD);

    List<String> sliceKeys = keyFields;
    if (mode == WriteMode.UPSERT) {
      sliceKeys = new ArrayList<>(keyFields);
      sliceKeys.add(LookupsIndex.LOOKUP_FIELD);
    }

    WriteConfig cfg =
        new WriteConfig(index, sliceFields, mode, sliceKeys, BATCH_SIZE, RefreshPolicy.IMMEDIATE);
    try (OpenSearchBulkWriter writer = new OpenSearchBulkWriter(client, cfg)) {
      for (Object[] row : rows) {
        Object[] tagged = new Object[row.length + 1];
        System.arraycopy(row, 0, tagged, 0, row.length);
        tagged[row.length] = uuid;
        writer.add(tagged);
      }
    }
  }

  private static String newUuid() {
    return UUID.randomUUID().toString();
  }

  private static String lookupFilter(String uuid) {
    return "{\"term\":{\"" + LookupsIndex.LOOKUP_FIELD + "\":\"" + uuid + "\"}}";
  }

  private static void addFilteredAlias(NodeClient client, String alias, String index, String uuid) {
    IndicesAliasesRequest req = new IndicesAliasesRequest();
    req.addAliasAction(AliasActions.add().index(index).alias(alias).filter(lookupFilter(uuid)));
    client.admin().indices().aliases(req).actionGet();
  }

  /**
   * Atomically move the alias from its current backing indices onto {@code newIndex} filtered on
   * {@code uuid}. The remove and add actions are one aliases request, applied as a single
   * cluster-state update, so a reader resolves either the whole old slice or the whole new one.
   */
  private static void repointFilteredAlias(
      NodeClient client, String alias, List<String> oldIndices, String newIndex, String uuid) {
    IndicesAliasesRequest req = new IndicesAliasesRequest();
    for (String old : oldIndices) {
      req.addAliasAction(AliasActions.remove().index(old).alias(alias));
    }
    req.addAliasAction(AliasActions.add().index(newIndex).alias(alias).filter(lookupFilter(uuid)));
    client.admin().indices().aliases(req).actionGet();
  }

  /** What the lookup name currently resolves to. */
  private enum Kind {
    ABSENT,
    /** A concrete index blocks a same-name alias, so it is refused. */
    INDEX,
    ALIAS
  }

  /**
   * Resolved state of the lookup name. For {@link Kind#ALIAS}, {@code aliasIndices} are the backing
   * indices, {@code filtered} is whether the alias carries a filter, and {@code lookupUuid} is the
   * {@code __lookup} discriminant parsed from that filter (null when the filter is not a {@code
   * __lookup} term).
   */
  private record Target(
      Kind kind, boolean filtered, List<String> aliasIndices, @Nullable String lookupUuid) {
    /** The index the alias currently points at (first, for the single-index lookup case). */
    String primaryIndex() {
      return aliasIndices.isEmpty() ? null : aliasIndices.get(0);
    }
  }

  /**
   * Probe the target at the indices level (no {@code cluster:monitor/state}): an alias is detected
   * via get-aliases, a concrete index via get-index.
   */
  private static Target resolveTarget(NodeClient client, String name) {
    Map<String, List<AliasMetadata>> aliases = getAliases(client, name);
    List<String> indices = new ArrayList<>();
    boolean filtered = false;
    String uuid = null;
    for (Map.Entry<String, List<AliasMetadata>> e : aliases.entrySet()) {
      for (AliasMetadata m : e.getValue()) {
        if (name.equals(m.alias())) {
          indices.add(e.getKey());
          if (m.filter() != null) {
            filtered = true;
            String parsed = extractLookupUuid(m.filter().string());
            if (parsed != null) {
              uuid = parsed;
            }
          }
        }
      }
    }
    if (!indices.isEmpty()) {
      return new Target(Kind.ALIAS, filtered, indices, uuid);
    }
    GetIndexResponse index = getIndex(client, name);
    if (index == null) {
      return new Target(Kind.ABSENT, false, List.of(), null);
    }
    return new Target(Kind.INDEX, false, List.of(), null);
  }

  private static @Nullable String extractLookupUuid(@Nullable String filterJson) {
    if (filterJson == null) {
      return null;
    }
    Matcher m = LOOKUP_UUID.matcher(filterJson);
    return m.find() ? m.group(1) : null;
  }

  private static Map<String, List<AliasMetadata>> getAliases(NodeClient client, String name) {
    try {
      return client
          .admin()
          .indices()
          .getAliases(new GetAliasesRequest(name))
          .actionGet()
          .getAliases();
    } catch (IndexNotFoundException e) {
      return Map.of();
    }
  }

  private static @Nullable GetIndexResponse getIndex(NodeClient client, String name) {
    try {
      return client.admin().indices().getIndex(new GetIndexRequest().indices(name)).actionGet();
    } catch (IndexNotFoundException e) {
      return null;
    }
  }
}
