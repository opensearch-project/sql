/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.ScriptedMetric;

/**
 * Parser for scripted metric aggregation responses. Extracts the final result from the reduce phase
 * of a scripted metric aggregation.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class ScriptedMetricParser implements MetricParser {

  private final String name;

  @Override
  public String getName() {
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> parse(Aggregation agg) {
    if (agg instanceof ScriptedMetric scriptedMetric) {
      // Extract the final result from the reduce script
      Object result = scriptedMetric.aggregation();
      // The reduce script for UDAF aggregation returns List<Map<String, Object>>
      // which represents the array of results. We wrap this in a single Map with
      // the aggregation field name as key, so the response is 1 row containing
      // the array that can be expanded by Uncollect in the query plan.
      if (result instanceof List) {
        return List.of(Map.of(name, result));
      }
      throw new IllegalArgumentException(
          String.format(
              "Expected List<Map<String, Object>> from scripted metric but got %s",
              result == null ? "null" : result.getClass().getSimpleName()));
    }
    throw new IllegalArgumentException(
        String.format(
            "Expected ScriptedMetric aggregation but got %s",
            agg == null ? "null" : agg.getClass().getSimpleName()));
  }
}
