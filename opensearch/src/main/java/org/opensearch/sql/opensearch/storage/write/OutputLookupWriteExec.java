/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Lifecycle for {@code outputlookup} (substrate <b>C2</b>: one plain index per lookup, weak/eventual
 * overwrite). A lookup named {@code <name>} <em>is</em> a plain index; readers ({@code
 * source=<name>}, {@code lookup <name>}) hit it directly. There is no alias, backing index, or uuid.
 *
 * <p>Every lookup index this command creates is tagged with the index metadata marker {@code
 * _meta.lookup = true}. Overwrite and append refuse to touch a plain index that lacks the marker
 * (i.e. a non-lookup index the user created for another purpose), so the command never clobbers
 * unrelated data; delete such an index first to reuse the name.
 *
 * <p><b>Consistency contract — weak/eventual (aligned with the merged Dashboards importer #11303,
 * which also writes lookups incrementally).</b> This command does not gold-plate read-continuity:
 *
 * <ul>
 *   <li><b>overwrite</b> ({@code append=false}) = delete the index and recreate it from the current
 *       result, then bulk write. The schema is fully replaced each run. There is a brief window
 *       where {@code <name>} is absent (a concurrent read sees index-not-found) and a crash
 *       mid-overwrite can lose the lookup — both within the weak/eventual contract; we deliberately
 *       do not reintroduce a blue/green alias swap to close that window (see the {@code
 *       feature/ppl-outputlookup-alias-swap} branch for the strong-consistency alternative).
 *   <li><b>append</b> ({@code append=true}) = bulk into the existing index. Incrementally visible,
 *       at-least-once; a keyed append is idempotent on re-run, a plain append may duplicate.
 * </ul>
 *
 * <p><b>Backward compatibility with the Dashboards data importer (#11303).</b> That feature realizes
 * a lookup as a <em>filtered alias</em> ({@code {term:{__lookup:<uuid>}}}) over a <em>shared</em>
 * index. The two formats coexist permanently; a lookup is migrated only when a name collision forces
 * it. Because a plain index cannot coexist with an alias of the same name, overwrite of a filtered
 * alias <em>migrates on touch</em>: it detaches the alias from the shared index and creates a
 * dedicated plain index {@code <name>} in its place. It <b>never deletes the shared index</b> (other
 * lookups' filtered aliases still use it); the orphaned {@code __lookup} slice is left in the shared
 * index. Reclaiming that slice (a periodic reaper keyed on the {@code __lookup} discriminant that
 * deletes a slice only once no alias references it) is intentionally out of scope here and tracked
 * as a follow-up PR. Append onto a filtered alias is refused (overwrite once to migrate first).
 * Reads of an un-migrated filtered alias remain compatible.
 *
 * <p><b>Permissions.</b> All operations run under the calling user's security context (verified: a
 * read-only user is denied). On the destination the caller needs {@code indices:data/write/bulk},
 * {@code indices:admin/create}, {@code indices:admin/delete}, and {@code indices:admin/get} (to
 * probe the target's state / lookup marker) — plus {@code indices:admin/aliases} only on the #11303
 * migration path. No cluster-level permission is required.
 */
public final class OutputLookupWriteExec {

  private static final int BATCH_SIZE = 1000;

  /** Index-metadata marker distinguishing a lookup this command owns from an arbitrary index. */
  private static final String META_KEY = "_meta";

  private static final String LOOKUP_MARKER = "lookup";

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
      String name,
      List<String> fields,
      Map<String, Object> mapping,
      WriteMode mode,
      List<String> keyFields,
      @Nullable Integer max,
      boolean overrideIfEmpty,
      boolean append,
      Enumerator<@Nullable Object> input) {

    List<Object[]> rows = new ArrayList<>();
    while (input.moveNext()) {
      if (max != null && rows.size() >= max) {
        break;
      }
      Object cur = input.current();
      rows.add(cur instanceof Object[] arr ? arr : new Object[] {cur});
    }

    Target target = resolveTarget(client, name);

    if (append) {
      switch (target.kind()) {
        case ABSENT:
          createIndex(client, name, mapping);
          break;
        case INDEX:
          if (!target.ownLookup()) {
            throw foreignIndexError(name);
          }
          // Append into our existing lookup index.
          break;
        case ALIAS:
          throw appendToAliasError(name, target.filtered());
        default:
          throw new IllegalStateException("unreachable target kind: " + target.kind());
      }
      writeRows(client, name, fields, mode, keyFields, rows);
      return rows.size();
    }

    // Overwrite: on an empty result, honor override_if_empty before touching anything.
    if (rows.isEmpty() && !overrideIfEmpty) {
      return 0;
    }
    switch (target.kind()) {
      case ABSENT:
        createIndex(client, name, mapping);
        break;
      case INDEX:
        if (!target.ownLookup()) {
          throw foreignIndexError(name);
        }
        // Weak/eventual replace: drop our old lookup index and recreate it with the current schema.
        // There is a brief absent window (contract-permitted); we do not blue/green it.
        deleteIndex(client, name);
        createIndex(client, name, mapping);
        break;
      case ALIAS:
        if (!target.filtered()) {
          // A plain (non-filtered) alias is not a recognized lookup; do not silently take it over.
          throw new IllegalArgumentException(
              "outputlookup destination [" + name + "] is an alias, not a lookup; refusing to"
                  + " overwrite it");
        }
        // Migrate-on-touch (#11303): detach the filtered alias from its (possibly shared) index —
        // NEVER delete that index — then create a dedicated plain index in its place. The orphaned
        // __lookup slice is left for a follow-up reaper.
        removeAlias(client, name, target.aliasIndices());
        createIndex(client, name, mapping);
        break;
      default:
        throw new IllegalStateException("unreachable target kind: " + target.kind());
    }
    writeRows(client, name, fields, mode, keyFields, rows);
    return rows.size();
  }

  private static IllegalArgumentException foreignIndexError(String name) {
    return new IllegalArgumentException(
        "outputlookup destination ["
            + name
            + "] is an existing non-lookup index (no lookup marker); refusing to overwrite it."
            + " Delete it first to reuse the name for a lookup");
  }

  private static IllegalArgumentException appendToAliasError(String name, boolean filtered) {
    if (filtered) {
      return new IllegalArgumentException(
          "outputlookup cannot append to lookup ["
              + name
              + "]: it is an alias over a shared index (e.g. a data-importer lookup); overwrite it"
              + " once to migrate it to a dedicated index, then append");
    }
    return new IllegalArgumentException(
        "outputlookup cannot append to ["
            + name
            + "]: it is an alias, not a lookup index; overwrite it first");
  }

  private static void writeRows(
      NodeClient client,
      String index,
      List<String> fields,
      WriteMode mode,
      List<String> keyFields,
      List<Object[]> rows) {
    WriteConfig cfg =
        new WriteConfig(index, fields, mode, keyFields, BATCH_SIZE, RefreshPolicy.IMMEDIATE);
    try (OpenSearchBulkWriter writer = new OpenSearchBulkWriter(client, cfg)) {
      for (Object[] row : rows) {
        writer.add(row);
      }
    }
  }

  /** Create the lookup index, tagging it with the {@code _meta.lookup} ownership marker. */
  private static void createIndex(NodeClient client, String index, Map<String, Object> mapping) {
    Map<String, Object> withMarker = new LinkedHashMap<>(mapping);
    withMarker.put(META_KEY, Map.of(LOOKUP_MARKER, true));
    client.admin().indices().create(new CreateIndexRequest(index).mapping(withMarker)).actionGet();
  }

  private static void deleteIndex(NodeClient client, String index) {
    try {
      client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    } catch (IndexNotFoundException ignored) {
      // already gone
    }
  }

  /** Detach the alias {@code name} from the given indices (used on the #11303 migration path). */
  private static void removeAlias(NodeClient client, String alias, List<String> indices) {
    IndicesAliasesRequest req = new IndicesAliasesRequest();
    for (String index : indices) {
      req.addAliasAction(AliasActions.remove().index(index).alias(alias));
    }
    client.admin().indices().aliases(req).actionGet();
  }

  /** What the lookup name currently resolves to. */
  private enum Kind {
    /** The name does not exist. */
    ABSENT,
    /** The name is a plain concrete index. */
    INDEX,
    /** The name is an alias (e.g. a #11303 filtered alias over a shared index). */
    ALIAS
  }

  /**
   * Resolved state of the lookup name. {@code ownLookup} is meaningful for {@link Kind#INDEX} (does
   * it carry our marker); {@code filtered} and {@code aliasIndices} for {@link Kind#ALIAS}.
   */
  private record Target(Kind kind, boolean ownLookup, boolean filtered, List<String> aliasIndices) {}

  /**
   * Probe the target at the indices level (no {@code cluster:monitor/state}): an alias is detected
   * via get-aliases, a concrete index and its lookup marker via get-index.
   */
  private static Target resolveTarget(NodeClient client, String name) {
    // Alias? get-aliases filters by alias name, so a non-empty result means <name> is an alias.
    Map<String, List<AliasMetadata>> aliases = getAliases(client, name);
    List<String> aliasIndices = new ArrayList<>();
    boolean filtered = false;
    for (Map.Entry<String, List<AliasMetadata>> e : aliases.entrySet()) {
      for (AliasMetadata m : e.getValue()) {
        if (name.equals(m.alias())) {
          aliasIndices.add(e.getKey());
          if (m.filter() != null) {
            filtered = true;
          }
        }
      }
    }
    if (!aliasIndices.isEmpty()) {
      return new Target(Kind.ALIAS, false, filtered, aliasIndices);
    }
    // Concrete index? get-index returns its mapping (with our marker) or throws if absent.
    GetIndexResponse index = getIndex(client, name);
    if (index == null) {
      return new Target(Kind.ABSENT, false, false, List.of());
    }
    return new Target(Kind.INDEX, hasLookupMarker(index, name), false, List.of());
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

  /** True iff the concrete index {@code name} carries our {@code _meta.lookup = true} marker. */
  private static boolean hasLookupMarker(GetIndexResponse index, String name) {
    MappingMetadata mapping = index.getMappings().get(name);
    if (mapping == null) {
      return false;
    }
    Object meta = mapping.getSourceAsMap().get(META_KEY);
    return meta instanceof Map && Boolean.TRUE.equals(((Map<?, ?>) meta).get(LOOKUP_MARKER));
  }
}
