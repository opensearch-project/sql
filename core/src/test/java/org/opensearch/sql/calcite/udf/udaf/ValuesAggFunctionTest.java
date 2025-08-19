/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.udf.udaf.ValuesAggFunction.ValuesAccumulator;

/** Unit tests for ValuesAggFunction. */
public class ValuesAggFunctionTest {

  private ValuesAggFunction valuesAggFunction;

  @BeforeEach
  public void setUp() {
    valuesAggFunction = new ValuesAggFunction();
  }

  @Test
  public void testInit() {
    ValuesAccumulator accumulator = valuesAggFunction.init();
    assertEquals(0, accumulator.size(), "New accumulator should be empty");
  }

  @Test
  public void testAddWithUniqueValues() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    accumulator = valuesAggFunction.add(accumulator, "apple");
    accumulator = valuesAggFunction.add(accumulator, "banana");
    accumulator = valuesAggFunction.add(accumulator, "cherry");

    assertEquals(3, accumulator.size(), "Should contain 3 unique values");
  }

  @Test
  public void testAddWithDuplicateValues() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    accumulator = valuesAggFunction.add(accumulator, "apple");
    accumulator = valuesAggFunction.add(accumulator, "banana");
    accumulator = valuesAggFunction.add(accumulator, "apple"); // duplicate
    accumulator = valuesAggFunction.add(accumulator, "banana"); // duplicate
    accumulator = valuesAggFunction.add(accumulator, "cherry");

    assertEquals(3, accumulator.size(), "Should contain only 3 unique values despite duplicates");
  }

  @Test
  public void testAddWithNullValues() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    accumulator = valuesAggFunction.add(accumulator, "apple");
    accumulator = valuesAggFunction.add(accumulator, (Object) null); // should be ignored
    accumulator = valuesAggFunction.add(accumulator, "banana");
    accumulator = valuesAggFunction.add(accumulator, (Object) null); // should be ignored

    assertEquals(2, accumulator.size(), "Should contain only 2 values, nulls ignored");
  }

  @Test
  public void testAddWithDifferentTypes() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    accumulator = valuesAggFunction.add(accumulator, 42); // integer
    accumulator = valuesAggFunction.add(accumulator, 3.14); // double
    accumulator = valuesAggFunction.add(accumulator, true); // boolean
    accumulator = valuesAggFunction.add(accumulator, "hello"); // string

    assertEquals(4, accumulator.size(), "Should contain 4 unique string representations");
  }

  @Test
  public void testResultWithLexicographicalOrder() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Add values in non-alphabetical order
    accumulator = valuesAggFunction.add(accumulator, "zebra");
    accumulator = valuesAggFunction.add(accumulator, "apple");
    accumulator = valuesAggFunction.add(accumulator, "banana");
    accumulator = valuesAggFunction.add(accumulator, "cherry");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) valuesAggFunction.result(accumulator);

    // Should be sorted lexicographically
    List<String> expected = Arrays.asList("apple", "banana", "cherry", "zebra");
    assertEquals(expected, result, "Result should be sorted lexicographically");
  }

  @Test
  public void testResultWithNumericStringSorting() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Add numeric values that should be sorted as strings
    accumulator = valuesAggFunction.add(accumulator, 100);
    accumulator = valuesAggFunction.add(accumulator, 20);
    accumulator = valuesAggFunction.add(accumulator, 3);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) valuesAggFunction.result(accumulator);

    // Should be sorted lexicographically as strings, not numerically
    List<String> expected = Arrays.asList("100", "20", "3");
    assertEquals(expected, result, "Numeric values should be sorted lexicographically as strings");
  }

  @Test
  public void testResultEmptyAccumulator() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) valuesAggFunction.result(accumulator);

    assertEquals(0, result.size(), "Empty accumulator should return empty list");
  }

  @Test
  public void testFullWorkflowWithDuplicatesAndSorting() {
    // Test the complete workflow: init -> add -> result
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Add values with duplicates in random order
    String[] inputValues = {"delta", "alpha", "beta", "alpha", "gamma", "beta", "delta"};

    for (String value : inputValues) {
      accumulator = valuesAggFunction.add(accumulator, value);
    }

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) valuesAggFunction.result(accumulator);

    // Should have unique values in lexicographical order
    List<String> expected = Arrays.asList("alpha", "beta", "delta", "gamma");
    assertEquals(expected, result, "Should return unique values in lexicographical order");
  }

  @Test
  public void testCaseSensitiveSorting() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Add values with different cases
    accumulator = valuesAggFunction.add(accumulator, "Apple");
    accumulator = valuesAggFunction.add(accumulator, "apple");
    accumulator = valuesAggFunction.add(accumulator, "APPLE");
    accumulator = valuesAggFunction.add(accumulator, "banana");
    accumulator = valuesAggFunction.add(accumulator, "Banana");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) valuesAggFunction.result(accumulator);

    assertEquals(5, result.size(), "Should treat different cases as different values");

    // Verify they are sorted lexicographically (uppercase comes before lowercase in ASCII)
    List<String> expected = Arrays.asList("APPLE", "Apple", "Banana", "apple", "banana");
    assertEquals(expected, result, "Should sort case-sensitively in lexicographical order");
  }

  @Test
  public void testLargeDataset() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Add many values with duplicates
    for (int i = 0; i < 1000; i++) {
      accumulator = valuesAggFunction.add(accumulator, "value" + (i % 100)); // creates duplicates
    }

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) valuesAggFunction.result(accumulator);

    assertEquals(100, result.size(), "Should contain exactly 100 unique values");

    // Verify first and last elements are sorted
    assertEquals("value0", result.get(0), "First element should be 'value0'");
    assertEquals("value99", result.get(99), "Last element should be 'value99'");
  }

  @Test
  public void testAddNoValues() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Test edge case where no values are passed
    accumulator = valuesAggFunction.add(accumulator);

    assertEquals(0, accumulator.size(), "Should remain empty when no values added");
  }

  @Test
  public void testAddEmptyValues() {
    ValuesAccumulator accumulator = valuesAggFunction.init();

    // Test edge case where null array is passed
    accumulator = valuesAggFunction.add(accumulator, (Object[]) null);

    assertEquals(0, accumulator.size(), "Should remain empty when null array passed");
  }
}
