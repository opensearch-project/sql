/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class MVFindFunctionImplTest {

  // Basic functionality tests

  @Test
  public void testMvfindWithSimpleMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.evalWithString(array, "banana");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithNoMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.evalWithString(array, "orange");
    assertNull(result);
  }

  @Test
  public void testMvfindWithFirstElementMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.evalWithString(array, "apple");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithLastElementMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.evalWithString(array, "cherry");
    assertEquals(2, result);
  }

  @Test
  public void testMvfindReturnsFirstMatch() {
    List<Object> array = Arrays.asList("test1", "test2", "test3");
    Object result = MVFindFunctionImpl.evalWithString(array, "test");
    assertEquals(0, result); // Returns first match, not all
  }

  // Null handling tests

  @Test
  public void testMvfindWithNullArray() {
    Object result = MVFindFunctionImpl.evalWithString(null, "pattern");
    assertNull(result);
  }

  @Test
  public void testMvfindWithNullRegex() {
    List<Object> array = Arrays.asList("apple", "banana");
    Object result = MVFindFunctionImpl.evalWithString(array, null);
    assertNull(result);
  }

  @Test
  public void testMvfindWithBothArgsNull() {
    Object result = MVFindFunctionImpl.evalWithString(null, null);
    assertNull(result);
  }

  @Test
  public void testMvfindWithNullElementInArray() {
    List<Object> array = Arrays.asList("apple", null, "banana");
    Object result = MVFindFunctionImpl.evalWithString(array, "banana");
    assertEquals(2, result);
  }

  // Edge cases

  @Test
  public void testMvfindWithEmptyArray() {
    List<Object> array = Collections.emptyList();
    Object result = MVFindFunctionImpl.evalWithString(array, "pattern");
    assertNull(result);
  }

  @Test
  public void testMvfindWithEmptyStringPattern() {
    List<Object> array = Arrays.asList("apple", "banana");
    Object result = MVFindFunctionImpl.evalWithString(array, "");
    assertEquals(0, result); // Empty pattern matches first element
  }

  @Test
  public void testMvfindWithSingleElementArray() {
    List<Object> array = Collections.singletonList("apple");
    Object result = MVFindFunctionImpl.evalWithString(array, "app");
    assertEquals(0, result);
  }

  // Regex pattern tests

  @Test
  public void testMvfindWithWildcardPattern() {
    List<Object> array = Arrays.asList("apple", "banana", "apricot");
    Object result = MVFindFunctionImpl.evalWithString(array, "ban.*");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithCharacterClass() {
    List<Object> array = Arrays.asList("error123", "info", "error456");
    Object result = MVFindFunctionImpl.evalWithString(array, "error[0-9]+");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithDigitClass() {
    List<Object> array = Arrays.asList("abc", "def123", "ghi");
    Object result = MVFindFunctionImpl.evalWithString(array, "\\d+");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithCaseInsensitiveFlag() {
    List<Object> array = Arrays.asList("Apple", "Banana", "Cherry");
    Object result = MVFindFunctionImpl.evalWithString(array, "(?i)banana");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithAnchorStart() {
    List<Object> array = Arrays.asList("hello", "say hello", "hello world");
    Object result = MVFindFunctionImpl.evalWithString(array, "^hello");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithAnchorEnd() {
    List<Object> array = Arrays.asList("world", "hello world", "world!");
    Object result = MVFindFunctionImpl.evalWithString(array, "world$");
    assertEquals(0, result);
  }

  // Case sensitivity

  @Test
  public void testMvfindIsCaseSensitiveByDefault() {
    List<Object> array = Arrays.asList("Apple", "banana", "Cherry");
    Object result = MVFindFunctionImpl.evalWithString(array, "apple");
    assertNull(result); // No match because case-sensitive
  }

  // Invalid regex patterns

  @Test
  public void testMvfindWithInvalidRegex() {
    List<Object> array = Arrays.asList("test");
    // Invalid regex should throw IllegalArgumentException (client error - 400)
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> MVFindFunctionImpl.evalWithString(array, "[invalid"));
    // Verify error message contains pattern details
    assertTrue(exception.getMessage().contains("Invalid regex pattern"));
    assertTrue(exception.getMessage().contains("[invalid"));
  }

  // Type conversion tests

  @Test
  public void testMvfindWithNumericElements() {
    List<Object> array = Arrays.asList(123, 456, 789);
    Object result = MVFindFunctionImpl.evalWithString(array, "456");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithMixedTypes() {
    List<Object> array = Arrays.asList("text", 123, "more text");
    Object result = MVFindFunctionImpl.evalWithString(array, "123");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithBooleanElements() {
    List<Object> array = Arrays.asList(true, false, true);
    Object result = MVFindFunctionImpl.evalWithString(array, "false");
    assertEquals(1, result);
  }

  // Type coercion tests (numeric patterns)

  @Test
  public void testMvfindWithNumericPatternAsString() {
    List<Object> array = Arrays.asList("apple", "404", "banana");
    // When called with string pattern
    Object result = MVFindFunctionImpl.evalWithString(array, "404");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithNumericPatternMatchingNumber() {
    List<Object> array = Arrays.asList("error", 404, "success");
    // Number in array matched by numeric pattern (toString conversion)
    Object result = MVFindFunctionImpl.evalWithString(array, "404");
    assertEquals(1, result);
  }
}
