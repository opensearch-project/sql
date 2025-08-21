/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * MIN_BY aggregate function implementation.
 * Returns the value from the first argument corresponding to the minimum value of the second argument.
 * Follows Spark SQL semantics: MIN_BY(value_field, order_field)
 */
public class MinByAggFunction implements UserDefinedAggFunction<MinByAggFunction.MinByAccumulator> {

  @Override
  public MinByAccumulator init() {
    return new MinByAccumulator();
  }

  @Override
  public Object result(MinByAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public MinByAccumulator add(MinByAccumulator acc, Object... values) {
    if (values.length < 2) {
      throw new IllegalArgumentException("MIN_BY requires exactly 2 arguments: value_field and order_field");
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

  public static class MinByAccumulator implements UserDefinedAggFunction.Accumulator {
    private Object minValue;
    private Comparable<Object> minOrderValue;

    public MinByAccumulator() {
      this.minValue = null;
      this.minOrderValue = null;
    }

    @Override
    public Object value(Object... argList) {
      return minValue;
    }

    @SuppressWarnings("unchecked")
    public void update(Object value, Object orderValue) {
      if (orderValue == null) {
        return;
      }
      
      try {
        Comparable<Object> comparableOrderValue = (Comparable<Object>) orderValue;
        
        if (minOrderValue == null || comparableOrderValue.compareTo(minOrderValue) < 0) {
          minValue = value;
          minOrderValue = comparableOrderValue;
        }
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "MIN_BY order field must be comparable. Got: " + orderValue.getClass().getSimpleName(), e);
      }
    }
  }
}
