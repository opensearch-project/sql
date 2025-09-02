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
 * Behavior: preserves insertion order, limits to 100 values, filters nulls.
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
    // Handle case where no values are passed
    if (values == null || values.length == 0) {
      return acc;
    }

    Object value = values[0];

    // Filter out null values and enforce 100-item limit
    if (value != null && acc.size() < DEFAULT_LIMIT) {
      // Convert value to string, handling all types safely
      String stringValue = String.valueOf(value);
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
