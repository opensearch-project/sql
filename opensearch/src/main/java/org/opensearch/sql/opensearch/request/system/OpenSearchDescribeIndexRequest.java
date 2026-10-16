/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.*;
import static org.opensearch.sql.opensearch.client.OpenSearchClient.META_CLUSTER_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.util.MergeRules.MergeRuleHelper;

@Log4j2
/** Describe index meta data request. */
public class OpenSearchDescribeIndexRequest implements OpenSearchSystemRequest {

  private static final String DEFAULT_TABLE_CAT = "opensearch";

  private static final Integer DEFAULT_NUM_PREC_RADIX = 10;

  private static final Integer DEFAULT_NULLABLE = 2;

  private static final String DEFAULT_IS_AUTOINCREMENT = "NO";

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  /** {@link OpenSearchRequest.IndexName}. */
  private final OpenSearchRequest.IndexName indexName;

  private final LangSpec langSpec;

  /** Whether the Calcite engine is enabled, which decides the type name of some field types. */
  private final boolean calciteEngine;

  public OpenSearchDescribeIndexRequest(OpenSearchClient client, String indexName) {
    this(client, new OpenSearchRequest.IndexName(indexName));
  }

  public OpenSearchDescribeIndexRequest(
      OpenSearchClient client, String indexName, LangSpec langSpec) {
    this(client, new OpenSearchRequest.IndexName(indexName), langSpec);
  }

  public OpenSearchDescribeIndexRequest(
      OpenSearchClient client, OpenSearchRequest.IndexName indexName) {
    this(client, indexName, LangSpec.SQL_SPEC);
  }

  public OpenSearchDescribeIndexRequest(
      OpenSearchClient client, OpenSearchRequest.IndexName indexName, LangSpec langSpec) {
    this(client, indexName, langSpec, false);
  }

  /**
   * @param calciteEngine whether the Calcite engine is enabled; PPL names a knn_vector field {@code
   *     vector} only then
   */
  public OpenSearchDescribeIndexRequest(
      OpenSearchClient client, String indexName, LangSpec langSpec, boolean calciteEngine) {
    this(client, new OpenSearchRequest.IndexName(indexName), langSpec, calciteEngine);
  }

  public OpenSearchDescribeIndexRequest(
      OpenSearchClient client,
      OpenSearchRequest.IndexName indexName,
      LangSpec langSpec,
      boolean calciteEngine) {
    this.client = client;
    this.indexName = indexName;
    this.langSpec = langSpec;
    this.calciteEngine = calciteEngine;
  }

  /**
   * search all the index in the data store.
   *
   * @return list of {@link ExprValue}
   */
  @Override
  public List<ExprValue> search() {
    List<ExprValue> results = new ArrayList<>();
    Map<String, String> meta = client.meta();
    int pos = 0;
    for (Map.Entry<String, OpenSearchDataType> entry :
        OpenSearchDataType.traverseAndFlatten(getFieldTypes()).entrySet()) {
      results.add(
          row(
              entry.getKey(),
              entry.getValue().describeTypeName(langSpec, calciteEngine).toLowerCase(Locale.ROOT),
              pos++,
              clusterName(meta)));
    }
    return results;
  }

  /**
   * Get the mapping of field and type.
   *
   * @return mapping of field and type.
   */
  // TODO possible collision if two indices have fields with the same name and different mappings
  public Map<String, OpenSearchDataType> getFieldTypes() {
    Map<String, OpenSearchDataType> fieldTypes = new HashMap<>();
    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName.getIndexNames());
    if (indexMappings.size() <= 1) {
      for (IndexMapping indexMapping : indexMappings.values()) {
        fieldTypes.putAll(indexMapping.getFieldMappings());
      }
    } else {
      // Merge deep copies: MergeRuleHelper mutates the field mappings in place, and a fetched
      // mapping must not be altered by being merged.
      for (Map.Entry<String, IndexMapping> indexMapping : indexMappings.entrySet()) {
        Map<String, OpenSearchDataType> copy = deepCopy(indexMapping.getValue().getFieldMappings());
        inIndex(copy, indexMapping.getKey());
        MergeRuleHelper.merge(fieldTypes, copy);
      }
    }
    return fieldTypes;
  }

  /**
   * Record the index on each knn_vector type of a copied mapping, so that the merged type keeps
   * every index's own vector mapping (see {@link OpenSearchKnnVectorType#inIndex}).
   */
  private static void inIndex(Map<String, OpenSearchDataType> mappings, String index) {
    for (Map.Entry<String, OpenSearchDataType> entry : mappings.entrySet()) {
      if (entry.getValue() instanceof OpenSearchKnnVectorType vector) {
        entry.setValue(vector.inIndex(index));
      }
      inIndex(entry.getValue().getProperties(), index);
    }
  }

  /** Copy a field-mapping map so an in-place merge cannot mutate the source types. */
  private static Map<String, OpenSearchDataType> deepCopy(
      Map<String, OpenSearchDataType> mappings) {
    Map<String, OpenSearchDataType> copy = new LinkedHashMap<>();
    mappings.forEach((field, type) -> copy.put(field, type.cloneDeep()));
    return copy;
  }

  /**
   * Get the minimum of the max result windows of the indices.
   *
   * @return max result window
   */
  public Integer getMaxResultWindow() {
    return client.getIndexMaxResultWindows(indexName.getIndexNames()).values().stream()
        .min(Integer::compare)
        .get();
  }

  private ExprTupleValue row(String fieldName, String fieldType, int position, String clusterName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CAT", stringValue(clusterName));
    valueMap.put("TABLE_NAME", stringValue(indexName.toString()));
    valueMap.put("COLUMN_NAME", stringValue(fieldName));
    // todo
    valueMap.put("TYPE_NAME", stringValue(fieldType));
    valueMap.put("NUM_PREC_RADIX", integerValue(DEFAULT_NUM_PREC_RADIX));
    valueMap.put("NULLABLE", integerValue(DEFAULT_NULLABLE));
    // There is no deterministic position of column in table
    valueMap.put("ORDINAL_POSITION", integerValue(position));
    // TODO Defaulting to unknown, need to check this
    valueMap.put("IS_NULLABLE", stringValue(""));
    // Defaulting to "NO"
    valueMap.put("IS_AUTOINCREMENT", stringValue(DEFAULT_IS_AUTOINCREMENT));
    // TODO Defaulting to unknown, need to check
    valueMap.put("IS_GENERATEDCOLUMN", stringValue(""));
    return new ExprTupleValue(valueMap);
  }

  private String clusterName(Map<String, String> meta) {
    return meta.getOrDefault(META_CLUSTER_NAME, DEFAULT_TABLE_CAT);
  }

  @Override
  public String toString() {
    return "OpenSearchDescribeIndexRequest{indexName='" + indexName + "\'}";
  }
}
