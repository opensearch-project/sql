/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/** FIRST aggregation function - returns the first non-null value in document order. */
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
    if (candidateValue != null && !acc.hasValue()) {
      acc.setValue(candidateValue);
    }
    return acc;
  }

  public static class FirstAccumulator implements Accumulator {
    private Object first;
    private boolean hasValue;

    public FirstAccumulator() {
      this.first = null;
      this.hasValue = false;
    }

    public void setValue(Object value) {
      if (!hasValue) {
        this.first = value;
        this.hasValue = true;
      }
    }

    public boolean hasValue() {
      return hasValue;
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
