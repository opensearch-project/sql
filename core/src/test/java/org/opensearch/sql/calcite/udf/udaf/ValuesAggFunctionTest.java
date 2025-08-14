/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ValuesAggFunctionTest {

  @Test
  void testValuesAggregation() {
    ValuesAggFunction function = new ValuesAggFunction();
    ValuesAggFunction.ValuesAccumulator accumulator = function.init();

    // Add values to accumulator
    function.add(accumulator, "banana");
    function.add(accumulator, "apple");
    function.add(accumulator, "cherry");
    function.add(accumulator, "apple"); // duplicate, should be removed

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(3, result.size());
    // Should be in lexicographical order
    assertEquals("apple", result.get(0));
    assertEquals("banana", result.get(1));
    assertEquals("cherry", result.get(2));
  }

  @Test
  void testValuesAggregationWithNulls() {
    ValuesAggFunction function = new ValuesAggFunction();
    ValuesAggFunction.ValuesAccumulator accumulator = function.init();

    // Add values including nulls
    function.add(accumulator, "banana");
    function.add(accumulator, (Object) null); // should be filtered out
    function.add(accumulator, "apple");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(2, result.size());
    assertEquals("apple", result.get(0));
    assertEquals("banana", result.get(1));
  }

  @Test
  void testLexicographicalOrder() {
    ValuesAggFunction function = new ValuesAggFunction();
    ValuesAggFunction.ValuesAccumulator accumulator = function.init();

    // Add numbers as strings to test lexicographical ordering
    function.add(accumulator, "10");
    function.add(accumulator, "2");
    function.add(accumulator, "1");
    function.add(accumulator, "20");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(4, result.size());
    // Lexicographical order: "1", "10", "2", "20"
    assertEquals("1", result.get(0));
    assertEquals("10", result.get(1));
    assertEquals("2", result.get(2));
    assertEquals("20", result.get(3));
  }

  @Test
  void testStringConversion() {
    ValuesAggFunction function = new ValuesAggFunction();
    ValuesAggFunction.ValuesAccumulator accumulator = function.init();

    // Add non-string values that should be converted
    function.add(accumulator, 123);
    function.add(accumulator, 45.67);
    function.add(accumulator, true);
    function.add(accumulator, 123); // duplicate

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertEquals(3, result.size());
    // Lexicographical order: "123", "45.67", "true"
    assertEquals("123", result.get(0));
    assertEquals("45.67", result.get(1));
    assertEquals("true", result.get(2));
  }

  @Test
  void testEmptyAggregation() {
    ValuesAggFunction function = new ValuesAggFunction();
    ValuesAggFunction.ValuesAccumulator accumulator = function.init();

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    assertTrue(result.isEmpty());
  }

  @Test
  void testNoLimit() {
    ValuesAggFunction function = new ValuesAggFunction();
    ValuesAggFunction.ValuesAccumulator accumulator = function.init();

    // Add many unique values (no limit for values() function)
    for (int i = 0; i < 200; i++) {
      function.add(accumulator, "value" + String.format("%03d", i));
    }

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) function.result(accumulator);

    // Should contain all 200 unique values
    assertEquals(200, result.size());
    assertEquals("value000", result.get(0));
    assertEquals("value199", result.get(199));
  }
}
