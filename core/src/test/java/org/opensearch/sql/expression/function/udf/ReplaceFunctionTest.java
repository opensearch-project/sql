/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link ReplaceFunction}. */
public class ReplaceFunctionTest {

  @Test
  public void testBasicReplacementAndRegexPatterns() {
    // Literal replacement
    assertEquals("Mane", ReplaceFunction.replaceRegex("Jane", "J", "M"));
    assertEquals("heLLo", ReplaceFunction.replaceRegex("hello", "l", "L"));

    // Regex patterns - all occurrences replaced
    assertEquals("test", ReplaceFunction.replaceRegex("test123", "\\d+", ""));
    assertEquals("XXX-XXX-XXXX", ReplaceFunction.replaceRegex("123-456-7890", "\\d", "X"));
    assertEquals("123456", ReplaceFunction.replaceRegex("abc123xyz456", "[a-z]+", ""));
    assertEquals("hello_world", ReplaceFunction.replaceRegex("hello world", " ", "_"));
  }

  @Test
  public void testRegexWithCaptureGroupsAndBackreferences() {
    // Swap month/day in date using capture groups and backreferences
    assertEquals(
        "14/1/2023",
        ReplaceFunction.replaceRegex("1/14/2023", "^(\\d{1,2})/(\\d{1,2})/", "\\2/\\1/"));

    // Swap words using regex groups
    assertEquals(
        "World Hello", ReplaceFunction.replaceRegex("Hello World", "(\\w+) (\\w+)", "\\2 \\1"));

    // Reuse same regex group multiple times
    assertEquals(
        "11-22-33",
        ReplaceFunction.replaceRegex("1-2-3", "(\\d)-(\\d)-(\\d)", "\\1\\1-\\2\\2-\\3\\3"));
  }

  @Test
  public void testEdgeCases() {
    // Null inputs
    assertNull(ReplaceFunction.replaceRegex(null, "test", "replacement"));
    assertNull(ReplaceFunction.replaceRegex("test", null, "replacement"));
    assertNull(ReplaceFunction.replaceRegex("test", "pattern", null));

    // No match returns original
    assertEquals("hello", ReplaceFunction.replaceRegex("hello", "xyz", "abc"));

    // Empty strings
    assertEquals("", ReplaceFunction.replaceRegex("", "test", "replacement"));
    assertEquals("", ReplaceFunction.replaceRegex("12345", "\\d+", ""));

    // Invalid regex
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ReplaceFunction.replaceRegex("test", "[invalid", "replacement"));
    assertTrue(exception.getMessage().contains("Error in 'replace' function"));
  }

  @Test
  public void testRealWorldScenarios() {
    // Date format conversion: MM/DD/YYYY to DD/MM/YYYY
    assertEquals(
        "14/01/2023",
        ReplaceFunction.replaceRegex("01/14/2023", "^(\\d{2})/(\\d{2})/(\\d{4})$", "\\2/\\1/\\3"));

    // Phone number formatting
    assertEquals(
        "(123) 456-7890",
        ReplaceFunction.replaceRegex("1234567890", "(\\d{3})(\\d{3})(\\d{4})", "(\\1) \\2-\\3"));

    // Remove HTML tags
    assertEquals(
        "Hello World", ReplaceFunction.replaceRegex("<p>Hello <b>World</b></p>", "<[^>]+>", ""));

    // Clean special characters
    assertEquals("5551234567", ReplaceFunction.replaceRegex("(555) 123-4567", "[^0-9]", ""));
  }

  @Test
  public void testAdvancedRegexFeatures() {
    // Word boundaries
    assertEquals("The dog sat", ReplaceFunction.replaceRegex("The cat sat", "\\bcat\\b", "dog"));

    // Case insensitive with (?i) flag
    assertEquals("X", ReplaceFunction.replaceRegex("abc", "(?i)ABC", "X"));

    // Greedy vs non-greedy quantifiers
    assertEquals("X", ReplaceFunction.replaceRegex("<tag>content</tag>", "<.*>", "X"));
    assertEquals("XcontentX", ReplaceFunction.replaceRegex("<tag>content</tag>", "<.*?>", "X"));

    // Anchors
    assertEquals("Xhello", ReplaceFunction.replaceRegex("###hello", "^#+", "X"));
    assertEquals("helloX", ReplaceFunction.replaceRegex("hello###", "#+$", "X"));

    // Remove repeated characters
    assertEquals("a1b2c3", ReplaceFunction.replaceRegex("aaa111bbb222ccc333", "(\\w)\\1+", "\\1"));
  }

  @Test
  public void testEscapedBackslashes() {
    // Escaped backslash produces literal backslash
    assertEquals("a\\b\\c", ReplaceFunction.replaceRegex("a/b/c", "/", "\\\\"));

    // \\1 is literal backslash + 1, not a capture group
    assertEquals("a\\1b", ReplaceFunction.replaceRegex("axb", "x", "\\\\1"));
  }
}
