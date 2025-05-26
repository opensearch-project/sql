/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.*;
import static org.opensearch.sql.opensearch.client.OpenSearchClient.META_CLUSTER_NAME;
import static org.opensearch.sql.opensearch.util.MergeRuleUtils.checkWhetherToMerge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;

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
    this.client = client;
    this.indexName = indexName;
    this.langSpec = langSpec;
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
              (langSpec.language() == QueryType.PPL
                      ? langSpec.typeName(entry.getValue().getExprType())
                      : entry.getValue().legacyTypeName())
                  .toLowerCase(Locale.ROOT),
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
    Map<String, IndexMapping> indexMappings =
        client.getIndexMappings(getLocalIndexNames(indexName.getIndexNames()));
    if (indexMappings.size() <= 1) {
      for (IndexMapping indexMapping : indexMappings.values()) {
        fieldTypes.putAll(indexMapping.getFieldMappings());
      }
    } else {
      for (IndexMapping indexMapping : indexMappings.values()) {
        mergeObjectAndArrayInsideMap(fieldTypes, indexMapping.getFieldMappings());
      }
    }
    return fieldTypes;
  }

  /**
   * Get the minimum of the max result windows of the indices.
   *
   * @return max result window
   */
  public Integer getMaxResultWindow() {
    return client
        .getIndexMaxResultWindows(getLocalIndexNames(indexName.getIndexNames()))
        .values()
        .stream()
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

  /**
   * Return index names without "{cluster}:" prefix. Without the prefix, they refer to the indices
   * at the local cluster.
   *
   * @param indexNames a string array of index names
   * @return local cluster index names
   */
  private String[] getLocalIndexNames(String[] indexNames) {
    return Arrays.stream(indexNames)
        .map(name -> name.substring(name.indexOf(":") + 1))
        .toArray(String[]::new);
  }

  private String clusterName(Map<String, String> meta) {
    return meta.getOrDefault(META_CLUSTER_NAME, DEFAULT_TABLE_CAT);
  }

  /**
   * The function accept two map and merge them. It will merge object/nested DataType if they're
   * under same key
   *
   * @param target The target map we will merge into
   * @param source The candidate map
   */
  public static void mergeObjectAndArrayInsideMap(
      Map<String, OpenSearchDataType> target, Map<String, OpenSearchDataType> source) {
    for (Map.Entry<String, OpenSearchDataType> entry : source.entrySet()) {
      String key = entry.getKey();
      OpenSearchDataType value = entry.getValue();

      if (target.containsKey(key) && checkWhetherToMerge(value, target.get(key))) {
        OpenSearchDataType merged = target.get(key);
        mergeObjectAndArrayInsideMap(merged.getProperties(), value.getProperties());
        target.put(key, merged);
      } else {
        target.put(key, value);
      }
    }
  }

  @Override
  public String toString() {
    return "OpenSearchDescribeIndexRequest{indexName='" + indexName + "\'}";
  }
}
