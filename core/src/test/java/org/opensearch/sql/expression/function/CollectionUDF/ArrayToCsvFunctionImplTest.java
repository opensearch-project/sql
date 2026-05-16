/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ArrayToCsvFunctionImplTest {

  @Test
  void testArrayToCsvWithDefaultDelimiter() {
    List<Object> array = Arrays.asList("GET", "READ", "WRITE");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("GET,READ,WRITE", result);
  }

  @Test
  void testArrayToCsvWithCustomDelimiter() {
    List<Object> array = Arrays.asList("GET", "READ", "WRITE");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ", ");
    assertEquals("GET, READ, WRITE", result);
  }

  @Test
  void testArrayToCsvWithPipeDelimiter() {
    List<Object> array = Arrays.asList("GET", "READ", "WRITE");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, " | ");
    assertEquals("GET | READ | WRITE", result);
  }

  @Test
  void testArrayToCsvWithEmptyArray() {
    List<Object> array = Collections.emptyList();
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("", result);
  }

  @Test
  void testArrayToCsvWithNullArray() {
    String result = ArrayToCsvFunctionImpl.arrayToCsv(null, ",");
    assertNull(result);
  }

  @Test
  void testArrayToCsvWithNullElements() {
    List<Object> array = Arrays.asList("GET", null, "WRITE");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("GET,,WRITE", result);
  }

  @Test
  void testArrayToCsvWithSingleElement() {
    List<Object> array = Arrays.asList("GET");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("GET", result);
  }

  @Test
  void testArrayToCsvWithNumbers() {
    List<Object> array = Arrays.asList(1, 2, 3);
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("1,2,3", result);
  }

  @Test
  void testArrayToCsvWithMixedTypes() {
    List<Object> array = Arrays.asList("GET", 123, true);
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("GET,123,true", result);
  }

  @Test
  void testArrayToCsvWithNullDelimiter() {
    List<Object> array = Arrays.asList("a", "b", "c");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, null);
    assertEquals("a,b,c", result);
  }

  @Test
  void testArrayToCsvWithNullDelimiterAndSingleElement() {
    List<Object> array = Arrays.asList("GET");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, null);
    assertEquals("GET", result);
  }

  @Test
  void testArrayToCsvWithNullDelimiterAndEmptyArray() {
    List<Object> array = Collections.emptyList();
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, null);
    assertEquals("", result);
  }

  @Test
  void testArrayToCsvWithMultipleNullElements() {
    List<Object> array = Arrays.asList("a", null, null, "b");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("a,,,b", result);
  }

  @Test
  void testArrayToCsvWithLeadingNullElement() {
    List<Object> array = Arrays.asList(null, "a", "b");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals(",a,b", result);
  }

  @Test
  void testArrayToCsvWithTrailingNullElement() {
    List<Object> array = Arrays.asList("a", "b", null);
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals("a,b,", result);
  }

  @Test
  void testArrayToCsvWithAllNullElements() {
    List<Object> array = Arrays.asList(null, null, null);
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, ",");
    assertEquals(",,", result);
  }

  @Test
  void testArrayToCsvWithNullElementsAndCustomDelimiter() {
    List<Object> array = Arrays.asList("GET", null, "WRITE");
    String result = ArrayToCsvFunctionImpl.arrayToCsv(array, " | ");
    assertEquals("GET |  | WRITE", result);
  }
}
