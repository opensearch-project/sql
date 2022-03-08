/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
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
