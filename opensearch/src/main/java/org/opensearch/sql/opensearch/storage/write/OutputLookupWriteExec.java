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
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Lifecycle for {@code outputlookup}: schema inference, backing-index create, atomic alias swap for
 * overwrite, and delegation of row writes to {@link OpenSearchBulkWriter}. A lookup name is an
 * alias over a backing index; overwrite writes a fresh backing index and atomically repoints the
 * alias (readers always see a consistent lookup), while append writes into the current backing.
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

    List<Object[]> rows = new ArrayList<>();
    while (input.moveNext()) {
      if (max != null && rows.size() >= max) {
        break;
      }
      Object cur = input.current();
      rows.add(cur instanceof Object[] arr ? arr : new Object[] {cur});
    }

    if (append) {
      String backing = resolveBacking(client, alias);
      if (backing == null) {
        backing = freshBacking(alias);
        createBacking(client, backing, mapping);
        swapAlias(client, alias, backing, List.of());
      }
      writeRows(client, backing, fields, mode, keyFields, rows);
      return rows.size();
    }

    // Overwrite: on an empty result, honor override_if_empty before touching anything.
    List<String> oldBackings = resolveBackings(client, alias);
    if (rows.isEmpty() && !overrideIfEmpty) {
      return 0;
    }
    String backing = freshBacking(alias);
    createBacking(client, backing, mapping);
    writeRows(client, backing, fields, mode, keyFields, rows);
    swapAlias(client, alias, backing, oldBackings);
    for (String old : oldBackings) {
      deleteIndex(client, old);
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

  /** The single backing index the alias currently points at, or null if the alias does not exist. */
  private static @Nullable String resolveBacking(NodeClient client, String alias) {
    List<String> backings = resolveBackings(client, alias);
    return backings.isEmpty() ? null : backings.get(0);
  }

  private static List<String> resolveBackings(NodeClient client, String alias) {
    try {
      return new ArrayList<>(
          client
              .admin()
              .indices()
              .getAliases(new GetAliasesRequest(alias))
              .actionGet()
              .getAliases()
              .keySet());
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
