/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.agg;

import java.util.List;
import java.util.Map;
import org.opensearch.search.aggregations.Aggregations;

/**
 * OpenSearch Aggregation Response Parser.
 */
public interface OpenSearchAggregationResponseParser {

  /**
   * Parse the OpenSearch Aggregation Response.
   * @param aggregations Aggregations.
   * @return aggregation result.
   */
  List<Map<String, Object>> parse(Aggregations aggregations);
}
