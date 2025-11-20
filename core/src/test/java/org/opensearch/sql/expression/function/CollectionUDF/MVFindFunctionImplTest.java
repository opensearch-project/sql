/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for MVFindFunctionImpl. */
public class MVFindFunctionImplTest {

  // Basic functionality tests

  @Test
  public void testMvfindWithSimpleMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.eval(array, "banana");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithNoMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.eval(array, "orange");
    assertNull(result);
  }

  @Test
  public void testMvfindWithFirstElementMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.eval(array, "apple");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithLastElementMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "cherry");
    Object result = MVFindFunctionImpl.eval(array, "cherry");
    assertEquals(2, result);
  }

  // Null handling tests

  @Test
  public void testMvfindWithNullArray() {
    Object result = MVFindFunctionImpl.eval(null, "pattern");
    assertNull(result);
  }

  @Test
  public void testMvfindWithNullRegex() {
    List<Object> array = Arrays.asList("apple", "banana");
    Object result = MVFindFunctionImpl.eval(array, null);
    assertNull(result);
  }

  @Test
  public void testMvfindWithNullArgs() {
    Object result = MVFindFunctionImpl.eval((Object[]) null);
    assertNull(result);
  }

  @Test
  public void testMvfindWithInsufficientArgs() {
    Object result = MVFindFunctionImpl.eval(new Object[] {Arrays.asList("test")});
    assertNull(result);
  }

  @Test
  public void testMvfindWithNullElementInArray() {
    List<Object> array = Arrays.asList("apple", null, "banana");
    Object result = MVFindFunctionImpl.eval(array, "banana");
    assertEquals(2, result);
  }

  @Test
  public void testMvfindWithAllNullElements() {
    List<Object> array = Arrays.asList(null, null, null);
    Object result = MVFindFunctionImpl.eval(array, "pattern");
    assertNull(result);
  }

  // Edge cases

  @Test
  public void testMvfindWithEmptyArray() {
    List<Object> array = Collections.emptyList();
    Object result = MVFindFunctionImpl.eval(array, "pattern");
    assertNull(result);
  }

  @Test
  public void testMvfindWithEmptyStringPattern() {
    List<Object> array = Arrays.asList("apple", "banana");
    Object result = MVFindFunctionImpl.eval(array, "");
    // Empty pattern matches everything (finds at position 0 in "apple")
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithEmptyStringElement() {
    List<Object> array = Arrays.asList("apple", "", "banana");
    Object result = MVFindFunctionImpl.eval(array, "");
    assertEquals(0, result); // Empty pattern matches first element
  }

  @Test
  public void testMvfindWithSingleElementArray() {
    List<Object> array = Collections.singletonList("apple");
    Object result = MVFindFunctionImpl.eval(array, "app");
    assertEquals(0, result);
  }

  // Regex pattern tests

  @Test
  public void testMvfindWithPartialMatch() {
    List<Object> array = Arrays.asList("apple", "banana", "apricot");
    Object result = MVFindFunctionImpl.eval(array, "ban");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithWildcardPattern() {
    List<Object> array = Arrays.asList("apple", "banana", "apricot");
    Object result = MVFindFunctionImpl.eval(array, "ban.*");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithDotPlus() {
    List<Object> array = Arrays.asList("a", "ab", "abc");
    Object result = MVFindFunctionImpl.eval(array, "a.+");
    assertEquals(1, result); // Matches "ab"
  }

  @Test
  public void testMvfindWithCharacterClass() {
    List<Object> array = Arrays.asList("error123", "info", "error456");
    Object result = MVFindFunctionImpl.eval(array, "error[0-9]+");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithDigitClass() {
    List<Object> array = Arrays.asList("abc", "def123", "ghi");
    Object result = MVFindFunctionImpl.eval(array, "\\d+");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithWordBoundary() {
    List<Object> array = Arrays.asList("hello world", "helloworld", "hello");
    Object result = MVFindFunctionImpl.eval(array, "\\bhello\\b");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithAnchorStart() {
    List<Object> array = Arrays.asList("hello", "say hello", "hello world");
    Object result = MVFindFunctionImpl.eval(array, "^hello");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithAnchorEnd() {
    List<Object> array = Arrays.asList("world", "hello world", "world!");
    Object result = MVFindFunctionImpl.eval(array, "world$");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithAlternation() {
    List<Object> array = Arrays.asList("cat", "dog", "bird");
    Object result = MVFindFunctionImpl.eval(array, "dog|cat");
    assertEquals(0, result); // Finds "cat" first
  }

  @Test
  public void testMvfindWithOptionalQuantifier() {
    List<Object> array = Arrays.asList("color", "colour", "colors");
    Object result = MVFindFunctionImpl.eval(array, "colou?r");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithCaseInsensitiveFlag() {
    List<Object> array = Arrays.asList("Apple", "Banana", "Cherry");
    Object result = MVFindFunctionImpl.eval(array, "(?i)banana");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithNegatedCharacterClass() {
    List<Object> array = Arrays.asList("abc123", "abc", "123");
    Object result = MVFindFunctionImpl.eval(array, "[^0-9]+");
    assertEquals(0, result); // Matches "abc123" (has non-digits)
  }

  @Test
  public void testMvfindWithEscapedSpecialChars() {
    List<Object> array = Arrays.asList("test.log", "test-log", "testlog");
    Object result = MVFindFunctionImpl.eval(array, "test\\.log");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithGroup() {
    List<Object> array = Arrays.asList("abc", "ababab", "abab");
    Object result = MVFindFunctionImpl.eval(array, "(ab)+");
    assertEquals(0, result);
  }

  // Multiple matches - should return first

  @Test
  public void testMvfindReturnsFirstMatch() {
    List<Object> array = Arrays.asList("apple", "apricot", "application");
    Object result = MVFindFunctionImpl.eval(array, "app");
    assertEquals(0, result); // Returns first match, not all
  }

  @Test
  public void testMvfindWithMultipleMatchingElements() {
    List<Object> array = Arrays.asList("test1", "test2", "test3");
    Object result = MVFindFunctionImpl.eval(array, "test");
    assertEquals(0, result); // Returns first
  }

  // Case sensitivity

  @Test
  public void testMvfindIsCaseSensitiveByDefault() {
    List<Object> array = Arrays.asList("Apple", "banana", "Cherry");
    Object result = MVFindFunctionImpl.eval(array, "apple");
    assertNull(result); // No match because case-sensitive
  }

  @Test
  public void testMvfindWithExactCaseMatch() {
    List<Object> array = Arrays.asList("Apple", "banana", "Cherry");
    Object result = MVFindFunctionImpl.eval(array, "Apple");
    assertEquals(0, result);
  }

  // Invalid regex patterns

  @Test
  public void testMvfindWithInvalidRegex() {
    List<Object> array = Arrays.asList("test");
    assertThrows(
        RuntimeException.class,
        () -> {
          MVFindFunctionImpl.eval(array, "[invalid");
        });
  }

  @Test
  public void testMvfindWithUnclosedGroup() {
    List<Object> array = Arrays.asList("test");
    assertThrows(
        RuntimeException.class,
        () -> {
          MVFindFunctionImpl.eval(array, "(unclosed");
        });
  }

  // Type conversion tests

  @Test
  public void testMvfindWithNumericElements() {
    List<Object> array = Arrays.asList(123, 456, 789);
    Object result = MVFindFunctionImpl.eval(array, "456");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithMixedTypes() {
    List<Object> array = Arrays.asList("text", 123, "more text");
    Object result = MVFindFunctionImpl.eval(array, "123");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindWithBooleanElements() {
    List<Object> array = Arrays.asList(true, false, true);
    Object result = MVFindFunctionImpl.eval(array, "false");
    assertEquals(1, result);
  }

  @Test
  public void testMvfindNumericPattern() {
    List<Object> array = Arrays.asList("test123", "test456", "test");
    Object result = MVFindFunctionImpl.eval(array, "\\d{3}");
    assertEquals(0, result);
  }

  // Special regex features

  @Test
  public void testMvfindWithLookahead() {
    List<Object> array = Arrays.asList("test123", "test", "test456");
    Object result = MVFindFunctionImpl.eval(array, "test(?=\\d)");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithLookbehind() {
    List<Object> array = Arrays.asList("pretest", "test", "posttest");
    Object result = MVFindFunctionImpl.eval(array, "(?<=pre)test");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithNonCapturingGroup() {
    List<Object> array = Arrays.asList("apple", "application", "apply");
    Object result = MVFindFunctionImpl.eval(array, "app(?:le|ly)");
    assertEquals(0, result);
  }

  // Edge cases with whitespace

  @Test
  public void testMvfindWithWhitespace() {
    List<Object> array = Arrays.asList("hello world", "hello", "world");
    Object result = MVFindFunctionImpl.eval(array, "hello\\s+world");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithLeadingTrailingSpaces() {
    List<Object> array = Arrays.asList("  test  ", "test", "  test");
    Object result = MVFindFunctionImpl.eval(array, "^\\s+test");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithTab() {
    List<Object> array = Arrays.asList("hello\tworld", "hello world");
    Object result = MVFindFunctionImpl.eval(array, "hello\\tworld");
    assertEquals(0, result);
  }

  // Unicode and special characters

  @Test
  public void testMvfindWithUnicodeCharacters() {
    List<Object> array = Arrays.asList("café", "resume", "naïve");
    Object result = MVFindFunctionImpl.eval(array, "café");
    assertEquals(0, result);
  }

  @Test
  public void testMvfindWithSpecialCharacters() {
    List<Object> array = Arrays.asList("hello@world", "hello#world", "hello$world");
    Object result = MVFindFunctionImpl.eval(array, "hello@world");
    assertEquals(0, result);
  }

  // Boundary testing

  @Test
  public void testMvfindWithVeryLongArray() {
    Object[] elements = new Object[1000];
    for (int i = 0; i < 1000; i++) {
      elements[i] = "element" + i;
    }
    List<Object> array = Arrays.asList(elements);
    Object result = MVFindFunctionImpl.eval(array, "element500");
    assertEquals(500, result);
  }

  @Test
  public void testMvfindWithVeryLongPattern() {
    List<Object> array = Arrays.asList("a".repeat(1000));
    Object result = MVFindFunctionImpl.eval(array, "a{1000}");
    assertEquals(0, result);
  }
}
