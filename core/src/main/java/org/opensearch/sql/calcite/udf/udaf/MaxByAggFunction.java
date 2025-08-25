/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * MAX_BY aggregate function implementation. Returns the value from the first argument corresponding
 * to the maximum value of the second argument. Follows Spark SQL semantics: MAX_BY(value_field,
 * order_field)
 */
public class MaxByAggFunction implements UserDefinedAggFunction<MaxByAggFunction.MaxByAccumulator> {

  @Override
  public MaxByAccumulator init() {
    return new MaxByAccumulator();
  }

  @Override
  public Object result(MaxByAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public MaxByAccumulator add(MaxByAccumulator acc, Object... values) {
    if (values.length < 2) {
      throw new IllegalArgumentException(
          "MAX_BY requires exactly 2 arguments: value_field and order_field");
    }

    Object candidateValue = values[0];
    Object candidateOrderValue = values[1];

    // Skip null order values
    if (candidateOrderValue == null) {
      return acc;
    }

    acc.update(candidateValue, candidateOrderValue);
    return acc;
  }

  public static class MaxByAccumulator implements UserDefinedAggFunction.Accumulator {
    private Object maxValue;
    private Comparable<Object> maxOrderValue;

    public MaxByAccumulator() {
      this.maxValue = null;
      this.maxOrderValue = null;
    }

    @Override
    public Object value(Object... argList) {
      return maxValue;
    }

    @SuppressWarnings("unchecked")
    public void update(Object value, Object orderValue) {
      if (orderValue == null) {
        return;
      }

      try {
        Comparable<Object> comparableOrderValue = (Comparable<Object>) orderValue;

        if (maxOrderValue == null || comparableOrderValue.compareTo(maxOrderValue) > 0) {
          maxValue = value;
          maxOrderValue = comparableOrderValue;
        }
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "MAX_BY order field must be comparable. Got: " + orderValue.getClass().getSimpleName(),
            e);
      }
    }
  }
}
