/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Write lifecycle for {@code outputlookup}: publishes the result as a {@code __lookup=<uuid>} slice
 * in a per-lookup backing index behind a filtered alias, gap-free on overwrite but weak/eventual.
 */
public final class OutputLookupWriteExec {

  private static final Logger LOGGER = LogManager.getLogger(OutputLookupWriteExec.class);

  private static final int BATCH_SIZE = 5000;

  private OutputLookupWriteExec() {}

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

    if (max != null && (max < 1 || max > maxRows)) {
      throw new IllegalArgumentException(
          "outputlookup max must be between 1 and the operator ceiling"
              + " plugins.ppl.outputlookup.max_rows ("
              + maxRows
              + "), but was "
              + max);
    }

    String backingIndex = name + LookupsIndex.BACKING_SUFFIX;

    List<Object[]> rows = drain(input, max, maxRows);

    if (!append && rows.isEmpty() && !overrideIfEmpty) {
      return 0;
    }

    LookupsIndex.ensureExists(client, backingIndex);

    Target target = resolveTarget(client, name);
    switch (target.kind()) {
      case ABSENT:
        {
          // append reuses a deterministic per-lookup discriminant so concurrent first-appends
          // converge on one slice (no lost write); overwrite takes a fresh uuid (last-writer-wins).
          String uuid = append ? stableUuid(name) : newUuid();
          writeSlice(client, backingIndex, fields, mode, keyFields, rows, uuid);
          addFilteredAlias(client, name, backingIndex, uuid);
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
            writeSlice(client, backingIndex, fields, mode, keyFields, rows, uuid);
            repointFilteredAlias(client, name, target.aliasIndices(), backingIndex, uuid);
            // TODO(reaper, separate PR): the atomic repoint leaves the previous slice as an
            // orphan (a __lookup uuid in the backing index referenced by no alias). Crash-before-
            // repoint and concurrent same-name overwrite produce the same orphan shape. A reaper
            // reclaims them per backing index: enumerate distinct __lookup uuids, subtract the set
            // referenced by any filtered alias, delete_by_query the remainder. This info log is the
            // reaper's observability seam until then.
            LOGGER.info(
                "outputlookup overwrite of [{}] wrote a new slice into [{}] and repointed the"
                    + " alias; the previous slice(s) on {} are orphaned pending the reaper",
                name,
                backingIndex,
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
   * {@code max} truncates silently; the {@code maxRows} operator ceiling fails loud, writing
   * nothing.
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
   * Salts the keyed id with the slice uuid so keyed rows never collide across slices sharing an
   * index.
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

    // The slice is not yet published, so drop redundancy/durability/auto-visibility during the load
    // and restore them after the single refresh below (before the alias op): no replica writes, no
    // per-request translog fsync, no background refresh churn while filling the slice.
    applyLoadSettings(client, index);

    WriteConfig cfg =
        new WriteConfig(index, sliceFields, mode, sliceKeys, BATCH_SIZE, RefreshPolicy.NONE);
    try (OpenSearchBulkWriter writer = new OpenSearchBulkWriter(client, cfg)) {
      for (Object[] row : rows) {
        Object[] tagged = new Object[row.length + 1];
        System.arraycopy(row, 0, tagged, 0, row.length);
        tagged[row.length] = uuid;
        writer.add(tagged);
      }
    }
    // Refresh once after the whole slice is written rather than per bulk batch: overwrite publishes
    // the slice through the atomic alias repoint that follows, and append through this refresh, so
    // per-batch refresh is redundant and dominates write cost at scale.
    client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    restoreServeSettings(client, index);
  }

  private static void applyLoadSettings(NodeClient client, String index) {
    updateSettings(
        client,
        index,
        Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.translog.durability", "async")
            .put("index.refresh_interval", "-1")
            .build());
  }

  private static void restoreServeSettings(NodeClient client, String index) {
    updateSettings(
        client,
        index,
        Settings.builder()
            .put("index.number_of_replicas", 1)
            .put("index.translog.durability", "request")
            .putNull("index.refresh_interval")
            .build());
  }

  private static void updateSettings(NodeClient client, String index, Settings settings) {
    client
        .admin()
        .indices()
        .updateSettings(new UpdateSettingsRequest(index).settings(settings))
        .actionGet();
  }

  private static String newUuid() {
    return UUID.randomUUID().toString();
  }

  private static String stableUuid(String name) {
    return UUID.nameUUIDFromBytes(("outputlookup:" + name).getBytes(StandardCharsets.UTF_8))
        .toString();
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

  private enum Kind {
    ABSENT,
    /** A concrete index blocks a same-name alias, so it is refused. */
    INDEX,
    ALIAS
  }

  private record Target(
      Kind kind, boolean filtered, List<String> aliasIndices, @Nullable String lookupUuid) {
    String primaryIndex() {
      return aliasIndices.isEmpty() ? null : aliasIndices.get(0);
    }
  }

  /** Probes at the indices level (no {@code cluster:monitor/state}): get-aliases then get-index. */
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

  /** Structurally parses the {@code __lookup} value from an alias filter; null if not present. */
  private static @Nullable String extractLookupUuid(@Nullable String filterJson) {
    if (filterJson == null) {
      return null;
    }
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, filterJson)) {
      Map<String, Object> root = parser.map();
      if (!(root.get("term") instanceof Map<?, ?> term)) {
        return null;
      }
      Object value = term.get(LookupsIndex.LOOKUP_FIELD);
      if (value instanceof Map<?, ?> valueObject) { // {"term":{"__lookup":{"value":"<uuid>"}}}
        Object nested = valueObject.get("value");
        return nested == null ? null : nested.toString();
      }
      return value == null ? null : value.toString(); // {"term":{"__lookup":"<uuid>"}}
    } catch (IOException e) {
      return null;
    }
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
