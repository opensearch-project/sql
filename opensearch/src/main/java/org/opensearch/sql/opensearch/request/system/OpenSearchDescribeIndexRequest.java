/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.opensearch.client.OpenSearchClient.META_CLUSTER_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;

/**
 * Describe index meta data request.
 */
public class OpenSearchDescribeIndexRequest implements OpenSearchSystemRequest {

  private static final String DEFAULT_TABLE_CAT = "opensearch";

  private static final Integer DEFAULT_NUM_PREC_RADIX = 10;

  private static final Integer DEFAULT_NULLABLE = 2;

  private static final String DEFAULT_IS_AUTOINCREMENT = "NO";

  /**
   * Type mapping from OpenSearch data type to expression type in our type system in query
   * engine. TODO: geo, ip etc.
   */
  private static final Map<String, ExprType> OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("text", OpenSearchDataType.OPENSEARCH_TEXT)
          .put("text_keyword", OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD)
          .put("keyword", ExprCoreType.STRING)
          .put("byte", ExprCoreType.BYTE)
          .put("short", ExprCoreType.SHORT)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("half_float", ExprCoreType.FLOAT)
          .put("scaled_float", ExprCoreType.DOUBLE)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("date_nanos", ExprCoreType.TIMESTAMP)
          .put("ip", OpenSearchDataType.OPENSEARCH_IP)
          .put("geo_point", OpenSearchDataType.OPENSEARCH_GEO_POINT)
          .put("binary", OpenSearchDataType.OPENSEARCH_BINARY)
          .build();

  /**
   * OpenSearch client connection.
   */
  private final OpenSearchClient client;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final OpenSearchRequest.IndexName indexName;

  public OpenSearchDescribeIndexRequest(OpenSearchClient client, String indexName) {
    this(client, new OpenSearchRequest.IndexName(indexName));
  }

  public OpenSearchDescribeIndexRequest(OpenSearchClient client,
      OpenSearchRequest.IndexName indexName) {
    this.client = client;
    this.indexName = indexName;
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
    for (Map.Entry<String, ExprType> entry : getFieldTypes().entrySet()) {
      results.add(
          row(entry.getKey(), entry.getValue().legacyTypeName().toLowerCase(), pos++,
              clusterName(meta)));
    }
    return results;
  }

  /**
   * Get the mapping of field and type.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName.getIndexNames());
    for (IndexMapping indexMapping : indexMappings.values()) {
      fieldTypes
          .putAll(indexMapping.getAllFieldTypes(this::transformESTypeToExprType).entrySet().stream()
              .filter(entry -> !ExprCoreType.UNKNOWN.equals(entry.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
    return fieldTypes;
  }

  private ExprType transformESTypeToExprType(String openSearchType) {
    return OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(openSearchType, ExprCoreType.UNKNOWN);
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
    return "OpenSearchDescribeIndexRequest{"
        + "indexName='" + indexName + '\''
        + '}';
  }
}
