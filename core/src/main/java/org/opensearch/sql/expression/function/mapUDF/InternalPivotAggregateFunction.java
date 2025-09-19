/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.mapUDF;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * Internal PIVOT aggregate function for timechart command. Converts long form data (timestamp,
 * category, value) to short form (timestamp, MAP<category, value>). This function is used
 * internally by the timechart command and is not exposed to users.
 */
public class InternalPivotAggregateFunction
    implements UserDefinedAggFunction<InternalPivotAggregateFunction.PivotAccumulator> {

  @Override
  public PivotAccumulator init() {
    return new PivotAccumulator();
  }

  @Override
  public Object result(PivotAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public PivotAccumulator add(PivotAccumulator acc, Object... values) {
    if (values.length != 2) {
      throw new IllegalArgumentException(
          "INTERNAL_PIVOT function requires exactly 2 arguments: value and category");
    }

    Object value = values[0]; // The value to aggregate (e.g., count, avg result)
    String category = (String) values[1]; // The pivot category (e.g., status, host)

    acc.addValue(value, category);
    return acc;
  }

  /**
   * Accumulator for PIVOT aggregation that maintains a MAP<category, value>. For timechart, we
   * typically just store the value as-is since aggregation already happened in the first GROUP BY
   * stage.
   */
  public static class PivotAccumulator implements UserDefinedAggFunction.Accumulator {
    private final Map<String, Object> pivotMap = new HashMap<>();

    /**
     * Add a value for a specific category to the pivot map.
     *
     * @param value The aggregated value (count, sum, avg, etc.)
     * @param category The category name (status code, host name, etc.)
     */
    public void addValue(Object value, String category) {
      if (value != null && category != null) {
        // For timechart, we store the value directly since aggregation already happened
        // If we needed to aggregate further (e.g., sum multiple values for same category),
        // we would implement that logic here
        pivotMap.put(category, value);
      }
    }

    @Override
    public Object value(Object... args) {
      // Return the MAP<String, Object> for dynamic columns processing
      return pivotMap.isEmpty() ? null : pivotMap;
    }
  }
}
