/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Lifecycle for {@code outputlookup}: schema inference, backing-index create, atomic alias swap for
 * overwrite, and delegation of row writes to {@link OpenSearchBulkWriter}. A lookup name is an
 * alias over a backing index; overwrite writes a fresh backing index and atomically repoints the
 * alias (readers always see a consistent lookup), while append writes into the current backing.
 *
 * <p>Concurrency and cleanup: the alias swap is a single atomic cluster-state update, so concurrent
 * overwrites to the same lookup are last-writer-wins and never leave the alias inconsistent. An
 * in-process write or swap failure deletes its own fresh backing. Backings orphaned by a
 * coordinator crash between create and swap, or by a losing concurrent overwrite, are not reaped
 * here; the {@code alias__ol_uuid} naming convention lets a periodic reaper collect any backing not
 * currently aliased (a follow-up). Append writes the live backing directly and is therefore not
 * atomic (at-least-once); a keyed append is idempotent on re-run, a plain append duplicates.
 *
 * <p>Backward compatibility with the Dashboards data importer (opensearch-project/OpenSearch-
 * Dashboards#11303): that feature realizes a lookup as a <em>filtered alias</em> ({@code
 * {term:{__lookup:<uuid>}}}) over a <em>shared</em> backing index. Overwrite migrates such a lookup
 * onto a dedicated backing by repointing the alias to the fresh backing; it never deletes the old
 * backing when that backing is shared/filtered (deleting it would destroy the other lookups that
 * share it). Only our own {@code <alias>__ol_*} unfiltered backings are deleted. Append onto a
 * shared/filtered lookup is refused (overwrite once to migrate first). This is the go-forward
 * backing-index-per-lookup format; the shared-index/{@code __lookup} format is deprecated.
 *
 * <p>Permissions: all operations run under the calling user's security context (verified: a
 * read-only user is denied). Beyond the read permissions on the source, the caller needs, on the
 * destination lookup, {@code indices:data/write/bulk}, {@code indices:admin/create},
 * {@code indices:admin/aliases}, and {@code indices:admin/delete}; the concrete-index guard also
 * uses {@code cluster:monitor/state} (a narrower indices-level check could remove that cluster
 * requirement in a follow-up).
 */
public final class OutputLookupWriteExec {

  private static final int BATCH_SIZE = 1000;

  private OutputLookupWriteExec() {}

  /** Map the child row schema to an OpenSearch mapping. Pure and unit-testable. */
  public static Map<String, Object> inferMapping(RelDataType rowType) {
    Map<String, Object> properties = new LinkedHashMap<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      // Skip reserved metadata fields (_id, _routing, ...); OpenSearch rejects them in a mapping,
      // and the writer already excludes them from the document source.
      if (OpenSearchIndex.METADATAFIELD_TYPE_MAP.containsKey(field.getName())) {
        continue;
      }
      properties.put(field.getName(), Map.of("type", esType(field.getType().getSqlTypeName().name())));
    }
    return Map.of("properties", properties);
  }

  private static String esType(String sqlTypeName) {
    switch (sqlTypeName) {
      case "TINYINT":
      case "SMALLINT":
      case "INTEGER":
      case "BIGINT":
        return "long";
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
      case "DECIMAL":
        return "double";
      case "BOOLEAN":
        return "boolean";
      case "DATE":
      case "TIME":
      case "TIMESTAMP":
        return "date";
      default:
        return "keyword";
    }
  }

  /**
   * Drain the input, materialize it into the lookup per the outputlookup options, and return the
   * number of rows written.
   */
  public static long execute(
      NodeClient client,
      String alias,
      List<String> fields,
      Map<String, Object> mapping,
      WriteMode mode,
      List<String> keyFields,
      @Nullable Integer max,
      boolean overrideIfEmpty,
      boolean append,
      Enumerator<@Nullable Object> input) {

    ensureNotConcreteIndex(client, alias);

    List<Object[]> rows = new ArrayList<>();
    while (input.moveNext()) {
      if (max != null && rows.size() >= max) {
        break;
      }
      Object cur = input.current();
      rows.add(cur instanceof Object[] arr ? arr : new Object[] {cur});
    }

    if (append) {
      OldBacking current = resolveOldBacking(client, alias);
      String backing;
      if (current == null) {
        backing = freshBacking(alias);
        createBacking(client, backing, mapping);
        swapAlias(client, alias, backing, List.of());
      } else if (!isOwnPrivateBacking(alias, current)) {
        // The lookup is backed by a shared or filtered index (e.g. a data-importer lookup on a
        // shared backing with a __lookup discriminant). Appending our rows into that index would
        // either be invisible through the filtered alias (no discriminant) or mutate a shared
        // index; neither is safe. Overwrite migrates the lookup to a dedicated backing first.
        throw new IllegalArgumentException(
            "outputlookup cannot append to lookup ["
                + alias
                + "]: it is backed by a shared or filtered index (e.g. a data-importer lookup);"
                + " overwrite it once to migrate it to a dedicated backing, then append");
      } else {
        backing = current.index();
      }
      writeRows(client, backing, fields, mode, keyFields, rows);
      return rows.size();
    }

    // Overwrite: on an empty result, honor override_if_empty before touching anything.
    List<OldBacking> oldBackings = resolveOldBackings(client, alias);
    if (rows.isEmpty() && !overrideIfEmpty) {
      return 0;
    }
    List<String> oldNames = new ArrayList<>();
    for (OldBacking o : oldBackings) {
      oldNames.add(o.index());
    }
    String backing = freshBacking(alias);
    createBacking(client, backing, mapping);
    boolean swapped = false;
    try {
      writeRows(client, backing, fields, mode, keyFields, rows);
      swapAlias(client, alias, backing, oldNames);
      swapped = true;
    } finally {
      // If the write or swap failed the fresh backing is never aliased, so delete it here rather
      // than leaking an orphan. The old lookup is untouched (alias still points at it).
      if (!swapped) {
        deleteIndex(client, backing);
      }
    }
    // Delete ONLY our own private, unfiltered backings. A shared or filtered backing (e.g. a
    // data-importer lookup on a shared index) has just been migrated by repointing the alias to
    // the fresh backing above; deleting that index would destroy other lookups sharing it, so we
    // leave the old slice in place (a periodic reaper keyed on the __lookup discriminant can
    // reclaim it — a cross-repo deprecation follow-up).
    for (OldBacking old : oldBackings) {
      if (isOwnPrivateBacking(alias, old)) {
        deleteIndex(client, old.index());
      }
    }
    return rows.size();
  }

  private static void writeRows(
      NodeClient client,
      String backing,
      List<String> fields,
      WriteMode mode,
      List<String> keyFields,
      List<Object[]> rows) {
    WriteConfig cfg =
        new WriteConfig(backing, fields, mode, keyFields, BATCH_SIZE, RefreshPolicy.IMMEDIATE);
    try (OpenSearchBulkWriter writer = new OpenSearchBulkWriter(client, cfg)) {
      for (Object[] row : rows) {
        writer.add(row);
      }
    }
  }

  private static String freshBacking(String alias) {
    return alias + "__ol_" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static void createBacking(NodeClient client, String backing, Map<String, Object> mapping) {
    client.admin().indices().create(new CreateIndexRequest(backing).mapping(mapping)).actionGet();
  }

  /**
   * Refuse to write when the lookup name is already a concrete index (not an alias). Otherwise the
   * alias swap would fail with a confusing "index and alias with the same name" error, and it
   * guards the dest == source concrete-index case.
   */
  private static void ensureNotConcreteIndex(NodeClient client, String alias) {
    org.opensearch.action.admin.cluster.state.ClusterStateResponse state =
        client
            .admin()
            .cluster()
            .state(
                new org.opensearch.action.admin.cluster.state.ClusterStateRequest()
                    .clear()
                    .metadata(true)
                    .indices(alias)
                    .indicesOptions(org.opensearch.action.support.IndicesOptions.lenientExpandOpen()))
            .actionGet();
    if (state.getState().getMetadata().hasIndex(alias)) {
      throw new IllegalArgumentException(
          "outputlookup destination ["
              + alias
              + "] is an existing concrete index; it must be a lookup alias or not exist");
    }
  }

  /**
   * An index the lookup alias currently points at, plus whether the alias on it carries a filter. A
   * filtered alias is the data-importer's shared-index lookup shape ({@code __lookup} discriminant);
   * a plain alias over an {@code <alias>__ol_*} index is one we own.
   */
  private record OldBacking(String index, boolean filtered) {}

  /** True iff this backing is one we own outright: our naming convention and no alias filter. */
  private static boolean isOwnPrivateBacking(String alias, OldBacking backing) {
    return !backing.filtered() && backing.index().startsWith(alias + "__ol_");
  }

  /** The single backing the alias points at (with filter flag), or null if the alias is absent. */
  private static @Nullable OldBacking resolveOldBacking(NodeClient client, String alias) {
    List<OldBacking> all = resolveOldBackings(client, alias);
    return all.isEmpty() ? null : all.get(0);
  }

  private static List<OldBacking> resolveOldBackings(NodeClient client, String alias) {
    try {
      Map<String, List<AliasMetadata>> aliases =
          client.admin().indices().getAliases(new GetAliasesRequest(alias)).actionGet().getAliases();
      List<OldBacking> result = new ArrayList<>();
      for (Map.Entry<String, List<AliasMetadata>> e : aliases.entrySet()) {
        boolean filtered =
            e.getValue().stream().anyMatch(m -> alias.equals(m.alias()) && m.filter() != null);
        result.add(new OldBacking(e.getKey(), filtered));
      }
      return result;
    } catch (IndexNotFoundException e) {
      return List.of();
    }
  }

  private static void swapAlias(
      NodeClient client, String alias, String newBacking, List<String> oldBackings) {
    IndicesAliasesRequest req = new IndicesAliasesRequest();
    req.addAliasAction(AliasActions.add().index(newBacking).alias(alias));
    for (String old : oldBackings) {
      req.addAliasAction(AliasActions.remove().index(old).alias(alias));
    }
    client.admin().indices().aliases(req).actionGet();
  }

  private static void deleteIndex(NodeClient client, String index) {
    try {
      client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    } catch (IndexNotFoundException ignored) {
      // already gone
    }
  }
}
