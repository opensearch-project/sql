/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * LAST aggregation function - returns the last value in natural document order. Returns NULL if no
 * records exist, or if the field is NULL in the last record of the bucket.
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
    // Always update with the latest value, including NULL values
    // This ensures we return NULL if the last record has a NULL field value
    acc.setValue(candidateValue);
    return acc;
  }

  public static class LastAccumulator implements Accumulator {
    private Object last;

    public LastAccumulator() {
      this.last = null;
    }

    public void setValue(Object value) {
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
