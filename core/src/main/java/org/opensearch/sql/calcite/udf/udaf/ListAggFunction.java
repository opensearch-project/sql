/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * List aggregation function that collects values into an array preserving duplicates and order.
 * Implements SPL-compatible behavior:
 * - Converts all input values to strings
 * - Preserves insertion order
 * - Limits result to first 100 values
 * - Filters out null values
 */
public class ListAggFunction implements UserDefinedAggFunction<ListAggFunction.ListAccumulator> {

  private static final int DEFAULT_LIMIT = 100;

  @Override
  public ListAccumulator init() {
    return new ListAccumulator();
  }

  @Override
  public Object result(ListAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public ListAccumulator add(ListAccumulator acc, Object... values) {
    Object value = values[0];
    
    // Filter out null values (SPL behavior)
    if (value != null && acc.size() < DEFAULT_LIMIT) {
      // Convert to string (SPL behavior)
      String stringValue = value.toString();
      acc.add(stringValue);
    }
    
    return acc;
  }

  public static class ListAccumulator implements Accumulator {
    private final List<String> values;

    public ListAccumulator() {
      this.values = new ArrayList<>();
    }

    @Override
    public Object value(Object... argList) {
      return values;
    }

    public void add(String value) {
      values.add(value);
    }

    public int size() {
      return values.size();
    }
  }
}