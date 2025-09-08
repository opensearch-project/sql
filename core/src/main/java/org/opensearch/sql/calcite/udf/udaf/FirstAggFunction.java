/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * FIRST aggregation function - returns the first non-null value in natural document order. Returns
 * NULL if no records exist, or if all records have NULL values for the field.
 */
public class FirstAggFunction implements UserDefinedAggFunction<FirstAggFunction.FirstAccumulator> {

  @Override
  public FirstAccumulator init() {
    return new FirstAccumulator();
  }

  @Override
  public Object result(FirstAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public FirstAccumulator add(FirstAccumulator acc, Object... values) {
    Object candidateValue = values[0];
    // Only accept the first non-null value encountered
    // Skip null values to find the first actual value
    if (candidateValue != null) {
      acc.setValue(candidateValue);
    }
    return acc;
  }

  public static class FirstAccumulator implements Accumulator {
    private volatile Object first;
    private volatile boolean hasValue;

    public FirstAccumulator() {
      this.first = null;
      this.hasValue = false;
    }

    public synchronized void setValue(Object value) {
      if (!hasValue) {
        this.first = value;
        this.hasValue = true;
      }
    }

    @Override
    public Object value(Object... argList) {
      return first;
    }

    public int size() {
      return hasValue ? 1 : 0;
    }
  }
}
