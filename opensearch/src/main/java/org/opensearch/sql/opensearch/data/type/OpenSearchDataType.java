/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * The extension of ExprType in Elasticsearch.
 */
@RequiredArgsConstructor
public enum OpenSearchDataType implements ExprType {
  /**
   * OpenSearch Text. Rather than cast text to other types (STRING), leave it alone to prevent
   * cast_to_string(OPENSEARCH_TEXT).
   * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
   */
  OPENSEARCH_TEXT(Collections.singletonList(STRING), "string") {
    @Override
    public boolean shouldCast(ExprType other) {
      return false;
    }
  },

  /**
   * OpenSearch multi-fields which has text and keyword.
   * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
   */
  OPENSEARCH_TEXT_KEYWORD(Arrays.asList(STRING, OPENSEARCH_TEXT), "string") {
    @Override
    public boolean shouldCast(ExprType other) {
      return false;
    }
  },


  OPENSEARCH_IP(Arrays.asList(UNKNOWN), "ip"),

  OPENSEARCH_GEO_POINT(Arrays.asList(UNKNOWN), "geo_point"),

  OPENSEARCH_BINARY(Arrays.asList(UNKNOWN), "binary");

  /**
   * Bidirectional mapping between OpenSearch type name and ExprType.
   */
  private static final BiMap<String, ExprType> OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableBiMap.<String, ExprType>builder()
          .put("text", OPENSEARCH_TEXT)
          .put("text_keyword", OPENSEARCH_TEXT_KEYWORD)
          .put("keyword", ExprCoreType.STRING)
          .put("byte", ExprCoreType.BYTE)
          .put("short", ExprCoreType.SHORT)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("ip", OPENSEARCH_IP)
          .put("geo_point", OPENSEARCH_GEO_POINT)
          .put("binary", OPENSEARCH_BINARY)
          .build();

  /**
   * Mapping from extra OpenSearch type name which may map to same ExprType as above.
   */
  private static final Map<String, ExprType> EXTRA_OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("half_float", ExprCoreType.FLOAT)
          .put("scaled_float", ExprCoreType.DOUBLE)
          .put("date_nanos", ExprCoreType.TIMESTAMP)
          .build();

  /**
   * The mapping between Type and legacy JDBC type name.
   */
  private static final Map<ExprType, String> LEGACY_TYPE_NAME_MAPPING =
      new ImmutableMap.Builder<ExprType, String>()
          .put(OPENSEARCH_TEXT, "text")
          .put(OPENSEARCH_TEXT_KEYWORD, "text")
          .build();

  /**
   * Parent of current type.
   */
  private final List<ExprType> parents;
  /**
   * JDBC type name.
   */
  private final String jdbcType;

  /**
   * Convert OpenSearch type string to ExprType.
   * @param openSearchType OpenSearch type string
   * @return expr type
   */
  public static ExprType getExprType(String openSearchType) {
    if (OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.containsKey(openSearchType)) {
      return OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.get(openSearchType);
    }
    return EXTRA_OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(openSearchType, UNKNOWN);
  }

  /**
   * Convert ExprType to OpenSearch type string.
   * @param type expr type
   * @return OpenSearch type string
   */
  public static String getOpenSearchType(ExprType type) {
    return OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.inverse().get(type);
  }

  @Override
  public List<ExprType> getParent() {
    return parents;
  }

  @Override
  public String typeName() {
    return jdbcType;
  }

  @Override
  public String legacyTypeName() {
    return LEGACY_TYPE_NAME_MAPPING.getOrDefault(this, typeName());
  }
}
