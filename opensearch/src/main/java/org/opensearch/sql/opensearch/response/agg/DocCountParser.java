/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.sql.data.model.ExprIntegerValue;

/**
 * Parser for extracting doc_count from bucket aggregations to optimize count() functions.
 */
public class DocCountParser implements MetricParser {
  private final String name;

  public DocCountParser(String name) {
    this.name = name;
  }

  @Override
  public Map<String, Object> parse(Aggregation aggregation) {
    throw new UnsupportedOperationException(
        "DocCountParser should be used with bucket context, not aggregations");
  }

  /**
   * Parse doc_count from bucket map.
   *
   * @param bucket bucket map containing doc_count
   * @return Map with the count value
   */
  public Map<String, Object> parseBucket(Map<String, Object> bucket) {
    Object docCount = bucket.get("doc_count");
    int count = (docCount instanceof Number) ? ((Number) docCount).intValue() : 0;
    Map<String, Object> result = new HashMap<>();
    result.put(name, new ExprIntegerValue(count).value());
    return result;
  }

  @Override
  public String getName() {
    return name;
  }
}