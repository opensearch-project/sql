/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * LAST aggregation function - returns the last non-null value in natural document order. Returns
 * NULL if no records exist, or if all records have NULL values for the field.
 */
public class LastAggFunction implements UserDefinedAggFunction<LastAggFunction.LastAccumulator> {

  @Override
  public LastAccumulator init() {
    return new LastAccumulator();
  }

  @Override
  public Object result(LastAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public LastAccumulator add(LastAccumulator acc, Object... values) {
    Object candidateValue = values[0];
    // Only update with non-null values to keep the last non-null value
    // Skip null values to preserve the previous non-null value
    if (candidateValue != null) {
      acc.setValue(candidateValue);
    }
    return acc;
  }

  public static class LastAccumulator implements Accumulator {
    private volatile Object last;

    public LastAccumulator() {
      this.last = null;
    }

    public synchronized void setValue(Object value) {
      this.last = value;
    }

    @Override
    public Object value(Object... argList) {
      return last;
    }

    public int size() {
      return last != null ? 1 : 0;
    }
  }
}
