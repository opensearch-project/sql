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
 * <p>Behavior:
 *
 * <ul>
 *   <li>Returns unique values only (no duplicates)
 *   <li>Values are sorted in lexicographical order
 *   <li>Processes field values as strings (casts all inputs to strings)
 *   <li>Configurable limit via plugins.ppl.values.max.limit setting (0 = unlimited)
 *   <li>Supports only scalar data types (rejects STRUCT/ARRAY types)
 *   <li>Implementation uses TreeSet for automatic sorting and deduplication
 * </ul>
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

    // Get limit from second argument (passed from AST)
    int limit = 0; // Default to unlimited
    if (values.length > 1 && values[1] != null) {
      limit = (Integer) values[1];
    }

    // Filter out null values and check limit
    if (value != null && (limit == 0 || acc.size() < limit)) {
      // Convert value to string
      String stringValue = String.valueOf(value);
      acc.add(stringValue, limit);
    }

    return acc;
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

    public void add(String value, int limit) {
      if (limit == 0 || values.size() < limit) {
        values.add(value);
      }
    }

    public int size() {
      return values.size();
    }
  }
}
