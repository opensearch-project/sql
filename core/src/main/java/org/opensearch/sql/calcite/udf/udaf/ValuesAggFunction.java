/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * Values aggregation function that collects unique values into a lexicographically sorted array.
 * Implements SPL-compatible behavior:
 * - Converts all input values to strings
 * - Removes duplicates
 * - Returns results in lexicographical (dictionary) order
 * - No limit on number of unique values
 * - Filters out null values
 */
public class ValuesAggFunction implements UserDefinedAggFunction<ValuesAggFunction.ValuesAccumulator> {

  @Override
  public ValuesAccumulator init() {
    return new ValuesAccumulator();
  }

  @Override
  public Object result(ValuesAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public ValuesAccumulator add(ValuesAccumulator acc, Object... values) {
    Object value = values[0];
    
    // Filter out null values (SPL behavior)
    if (value != null) {
      // Convert to string (SPL behavior)
      String stringValue = value.toString();
      acc.add(stringValue);
    }
    
    return acc;
  }

  public static class ValuesAccumulator implements Accumulator {
    private final Set<String> uniqueValues;

    public ValuesAccumulator() {
      this.uniqueValues = new LinkedHashSet<>();
    }

    @Override
    public Object value(Object... argList) {
      // Return sorted list in lexicographical order (SPL behavior)
      List<String> sortedValues = uniqueValues.stream()
          .sorted()
          .collect(Collectors.toList());
      return sortedValues;
    }

    public void add(String value) {
      uniqueValues.add(value);
    }

    public int size() {
      return uniqueValues.size();
    }
  }
}