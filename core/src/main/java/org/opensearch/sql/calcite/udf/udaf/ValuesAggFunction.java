/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/**
 * VALUES aggregate function implementation. Returns distinct values from a field in lexicographical
 * order as a multivalue field.
 *
 * <p>Behavior: - Returns unique values only (no duplicates) - Values are sorted in lexicographical
 * order - Processes field values as strings (casts all inputs to strings) - No limit on number of
 * values (unlike LIST which has 100 limit) - Supports only scalar data types (rejects STRUCT/ARRAY
 * types) - Implementation uses TreeSet for automatic sorting and deduplication
 */
public class ValuesAggFunction
    implements UserDefinedAggFunction<ValuesAggFunction.ValuesAccumulator> {

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
    // Handle case where no values are passed
    if (values == null || values.length == 0) {
      return acc;
    }

    Object value = values[0];

    // Filter out null values
    if (value != null) {
      // Convert value to string, handling all types safely
      String stringValue = convertToString(value);
      acc.add(stringValue);
    }

    return acc;
  }

  /** Converts any value to its string representation. */
  private String convertToString(Object value) {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  public static class ValuesAccumulator implements Accumulator {
    private final Set<String> values;

    public ValuesAccumulator() {
      this.values = new TreeSet<>(); // TreeSet maintains sorted order and uniqueness
    }

    @Override
    public Object value(Object... argList) {
      return new ArrayList<>(values); // Return List<String> to match expected type
    }

    public void add(String value) {
      values.add(value);
    }

    public int size() {
      return values.size();
    }
  }
}
