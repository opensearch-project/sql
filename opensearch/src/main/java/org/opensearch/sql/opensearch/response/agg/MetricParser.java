/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.agg;

import java.util.Map;
import org.opensearch.search.aggregations.Aggregation;

/**
 * Metric Aggregation Parser.
 */
public interface MetricParser {

  /**
   * Get the name of metric parser.
   */
  String getName();

  /**
   * Parse the {@link Aggregation}.
   *
   * @param aggregation {@link Aggregation}
   * @return the map between metric name and metric value.
   */
  Map<String, Object> parse(Aggregation aggregation);
}
