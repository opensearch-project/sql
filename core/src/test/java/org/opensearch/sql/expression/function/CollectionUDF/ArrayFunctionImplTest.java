/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ArrayFunctionImpl.
 *
 * <p>These tests verify that the array() function correctly handles null elements inside arrays,
 * which is critical for the NOMV command's null filtering functionality via ARRAY_COMPACT.
 *
 * <p>The array() function uses NullPolicy.NONE, meaning it accepts null arguments and preserves
 * them inside the resulting array. This allows ARRAY_COMPACT to filter them out later.
 */
public class ArrayFunctionImplTest {

  @Test
  public void testArrayWithNoArguments() {
    Object result = ArrayFunctionImpl.internalCast(Collections.emptyList(), SqlTypeName.VARCHAR);
    assertNotNull(result, "Empty array should not be null");
    assertTrue(result instanceof List, "Result should be a List");
    assertEquals(0, ((List<?>) result).size(), "Empty array should have size 0");
  }

  @Test
  public void testArrayWithSingleElement() {
    Object result = ArrayFunctionImpl.internalCast(Arrays.asList("test"), SqlTypeName.VARCHAR);
    assertNotNull(result);
    assertTrue(result instanceof List);
    assertEquals(Arrays.asList("test"), result);
  }

  @Test
  public void testArrayWithMultipleElements() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList("a", "b", "c"), SqlTypeName.VARCHAR);
    assertNotNull(result);
    assertEquals(Arrays.asList("a", "b", "c"), result);
  }

  // ==================== NULL HANDLING TESTS ====================
  // These tests are critical for NOMV command's null filtering

  @Test
  public void testArrayWithNullInMiddle() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList("a", null, "b"), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array with null should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size(), "Array should preserve null element");
    assertEquals("a", list.get(0));
    assertNull(list.get(1), "Middle element should be null");
    assertEquals("b", list.get(2));
  }

  @Test
  public void testArrayWithNullAtBeginning() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList(null, "a", "b"), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array with null should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size(), "Array should preserve null element");
    assertNull(list.get(0), "First element should be null");
    assertEquals("a", list.get(1));
    assertEquals("b", list.get(2));
  }

  @Test
  public void testArrayWithNullAtEnd() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList("a", "b", null), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array with null should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size(), "Array should preserve null element");
    assertEquals("a", list.get(0));
    assertEquals("b", list.get(1));
    assertNull(list.get(2), "Last element should be null");
  }

  @Test
  public void testArrayWithMultipleNulls() {
    Object result =
        ArrayFunctionImpl.internalCast(
            Arrays.asList("a", null, "b", null, "c"), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array with nulls should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(5, list.size(), "Array should preserve all null elements");
    assertEquals("a", list.get(0));
    assertNull(list.get(1), "Second element should be null");
    assertEquals("b", list.get(2));
    assertNull(list.get(3), "Fourth element should be null");
    assertEquals("c", list.get(4));
  }

  @Test
  public void testArrayWithAllNulls() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList(null, null, null), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array of all nulls should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size(), "Array should preserve all null elements");
    assertNull(list.get(0));
    assertNull(list.get(1));
    assertNull(list.get(2));
  }

  @Test
  public void testArrayWithSingleNull() {
    Object result =
        ArrayFunctionImpl.internalCast(Collections.singletonList(null), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array with single null should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(1, list.size(), "Array should contain one null element");
    assertNull(list.get(0));
  }

  @Test
  public void testArrayWithMixedTypesAndNulls() {
    Object result =
        ArrayFunctionImpl.internalCast(
            Arrays.asList(1, null, "text", null, 3.14), SqlTypeName.VARCHAR);
    assertNotNull(result, "Array with mixed types and nulls should not return null");
    assertTrue(result instanceof List);
    List<?> list = (List<?>) result;
    assertEquals(5, list.size());
    assertEquals("1", list.get(0)); // Converted to string
    assertNull(list.get(1));
    assertEquals("text", list.get(2));
    assertNull(list.get(3));
    assertEquals("3.14", list.get(4)); // Converted to string
  }

  // ==================== INTEGRATION WITH NOMV WORKFLOW ====================
  // These tests verify the array works correctly in the NOMV workflow:
  // array(fields) -> array_compact(array) -> mvjoin(compacted, '\n') -> coalesce(result, '')

  @Test
  public void testArrayOutputCanBeProcessedByArrayCompact() {
    // Simulate: array(field1, null, field2) -> array_compact
    Object arrayResult =
        ArrayFunctionImpl.internalCast(
            Arrays.asList("value1", null, "value2"), SqlTypeName.VARCHAR);
    assertNotNull(arrayResult);
    assertTrue(arrayResult instanceof List);

    // Verify the array has the structure expected by array_compact
    List<?> list = (List<?>) arrayResult;
    assertEquals(3, list.size(), "Array should have 3 elements before compacting");

    // Simulate what array_compact would do (filter out nulls)
    List<?> compacted = list.stream().filter(item -> item != null).collect(Collectors.toList());
    assertEquals(2, compacted.size(), "After compacting, array should have 2 elements");
    assertEquals("value1", compacted.get(0));
    assertEquals("value2", compacted.get(1));
  }

  @Test
  public void testArrayWithAllNullsForNomvWorkflow() {
    // RFC Example 9: array(null, null, null) should allow NOMV to return ""
    Object arrayResult =
        ArrayFunctionImpl.internalCast(Arrays.asList(null, null, null), SqlTypeName.VARCHAR);
    assertNotNull(arrayResult);
    assertTrue(arrayResult instanceof List);

    List<?> list = (List<?>) arrayResult;
    assertEquals(3, list.size(), "Array should preserve all nulls");

    // Simulate array_compact - should result in empty array
    List<?> compacted =
        list.stream().filter(item -> item != null).collect(java.util.stream.Collectors.toList());
    assertEquals(
        0, compacted.size(), "After compacting all nulls, array should be empty for NOMV to use");
  }

  @Test
  public void testArrayPreservesNullsForRFCExample5() {
    // RFC Example 5: nomv should filter nulls
    // array('a', null, 'b') -> array_compact -> ['a', 'b'] -> mvjoin -> "a\nb"
    Object arrayResult =
        ArrayFunctionImpl.internalCast(Arrays.asList("a", null, "b"), SqlTypeName.VARCHAR);
    assertNotNull(arrayResult);

    List<?> list = (List<?>) arrayResult;
    assertEquals(3, list.size(), "Original array should have 3 elements");
    assertNull(list.get(1), "Middle element should be null");

    // After array_compact
    List<?> compacted =
        list.stream().filter(item -> item != null).collect(java.util.stream.Collectors.toList());
    assertEquals(2, compacted.size());
    assertEquals("a", compacted.get(0));
    assertEquals("b", compacted.get(1));
  }

  // ==================== EDGE CASE TESTS ====================

  @Test
  public void testArrayWithNumericTypes() {
    Object result = ArrayFunctionImpl.internalCast(Arrays.asList(1, 2, 3), SqlTypeName.INTEGER);
    assertNotNull(result);
    assertEquals(Arrays.asList(1, 2, 3), result);
  }

  @Test
  public void testArrayWithMixedNumericAndString() {
    Object result = ArrayFunctionImpl.internalCast(Arrays.asList(1, "two", 3), SqlTypeName.VARCHAR);
    assertNotNull(result);
    assertEquals(Arrays.asList("1", "two", "3"), result);
  }

  @Test
  public void testArrayWithEmptyStrings() {
    // Empty strings should be preserved (they are not null)
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList("a", "", "b"), SqlTypeName.VARCHAR);
    assertNotNull(result);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size());
    assertEquals("a", list.get(0));
    assertEquals("", list.get(1), "Empty string should be preserved");
    assertEquals("b", list.get(2));
  }

  @Test
  public void testArrayWithBooleanValues() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList(true, false, null), SqlTypeName.BOOLEAN);
    assertNotNull(result);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size());
    assertEquals(true, list.get(0));
    assertEquals(false, list.get(1));
    assertNull(list.get(2));
  }

  // ==================== TYPE CONVERSION TESTS ====================
  // Test that internalCast correctly handles type conversions while preserving nulls

  @Test
  public void testArrayWithDoubleTypePreservesNulls() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList(1.5, null, 2.7), SqlTypeName.DOUBLE);
    assertNotNull(result);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size());
    assertEquals(1.5, list.get(0));
    assertNull(list.get(1), "Null should be preserved during DOUBLE type conversion");
    assertEquals(2.7, list.get(2));
  }

  @Test
  public void testArrayWithFloatTypePreservesNulls() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList(1.5f, null, 2.7f), SqlTypeName.FLOAT);
    assertNotNull(result);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size());
    assertEquals(1.5f, list.get(0));
    assertNull(list.get(1), "Null should be preserved during FLOAT type conversion");
    assertEquals(2.7f, list.get(2));
  }

  @Test
  public void testArrayWithVarcharTypePreservesNulls() {
    Object result =
        ArrayFunctionImpl.internalCast(Arrays.asList("a", null, "b"), SqlTypeName.VARCHAR);
    assertNotNull(result);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size());
    assertEquals("a", list.get(0));
    assertNull(list.get(1), "Null should be preserved during VARCHAR type conversion");
    assertEquals("b", list.get(2));
  }

  @Test
  public void testArrayWithCharTypePreservesNulls() {
    Object result = ArrayFunctionImpl.internalCast(Arrays.asList("x", null, "y"), SqlTypeName.CHAR);
    assertNotNull(result);
    List<?> list = (List<?>) result;
    assertEquals(3, list.size());
    assertEquals("x", list.get(0));
    assertNull(list.get(1), "Null should be preserved during CHAR type conversion");
    assertEquals("y", list.get(2));
  }
}
