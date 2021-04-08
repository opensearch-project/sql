/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.opensearch.data.type;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;

/**
 * The extension of ExprType in Elasticsearch.
 */
@RequiredArgsConstructor
public enum OpenSearchDataType implements ExprType {
  /**
   * OpenSearch Text.
   * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
   */
  OPENSEARCH_TEXT(Collections.singletonList(STRING), "string"),

  /**
   * OpenSearch multi-fields which has text and keyword.
   * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
   */
  OPENSEARCH_TEXT_KEYWORD(Arrays.asList(STRING, OPENSEARCH_TEXT), "string"),


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
