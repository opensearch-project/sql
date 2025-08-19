/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ListAggFunctionTest {

  @Test
  void testListAggregation() {
    ListAggFunction function = new ListAggFunction();
    ListAggFunction.ListAccumulator accumulator = function.init();

    // Add values to accumulator
    function.add(accumulator, "apple");
    function.add(accumulator, "banana");
    function.add(accumulator, "apple"); // duplicate

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(3, result.size());
    assertEquals("apple", result.get(0));
    assertEquals("banana", result.get(1));
    assertEquals("apple", result.get(2)); // preserves duplicates
  }

  @Test
  void testListAggregationWithNulls() {
    ListAggFunction function = new ListAggFunction();
    ListAggFunction.ListAccumulator accumulator = function.init();

    // Add values including nulls
    function.add(accumulator, "apple");
    function.add(accumulator, (Object) null); // should be filtered out
    function.add(accumulator, "banana");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(2, result.size());
    assertEquals("apple", result.get(0));
    assertEquals("banana", result.get(1));
  }

  @Test
  void testListAggregationLimit() {
    ListAggFunction function = new ListAggFunction();
    ListAggFunction.ListAccumulator accumulator = function.init();

    // Add more than 100 values
    for (int i = 0; i < 150; i++) {
      function.add(accumulator, "value" + i);
    }

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    // Should be limited to 100 values
    assertEquals(100, result.size());
    assertEquals("value0", result.get(0));
    assertEquals("value99", result.get(99));
  }

  @Test
  void testStringConversion() {
    ListAggFunction function = new ListAggFunction();
    ListAggFunction.ListAccumulator accumulator = function.init();

    // Add non-string values that should be converted
    function.add(accumulator, 123);
    function.add(accumulator, 45.67);
    function.add(accumulator, true);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(3, result.size());
    assertEquals("123", result.get(0));
    assertEquals("45.67", result.get(1));
    assertEquals("true", result.get(2)); // boolean true now user-friendly "true"
  }

  @Test
  void testEmptyAggregation() {
    ListAggFunction function = new ListAggFunction();
    ListAggFunction.ListAccumulator accumulator = function.init();

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertTrue(result.isEmpty());
  }
}
