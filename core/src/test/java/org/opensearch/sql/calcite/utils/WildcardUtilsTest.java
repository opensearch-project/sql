/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WildcardUtilsTest {

  private List<String> availableFields;

  @BeforeEach
  void setUp() {
    availableFields =
        ImmutableList.of(
            "account_number",
            "firstname",
            "lastname",
            "balance",
            "age",
            "city",
            "state",
            "gender",
            "employer");
  }

  private void testPattern(String pattern, String fieldName, boolean expected) {
    boolean result = WildcardUtils.matchesWildcardPattern(pattern, fieldName);
    if (result != expected) {
      throw new AssertionError("Expected: " + expected + ", but got: " + result);
    }
  }

  private void testExpansion(String pattern, List<String> expectedFields) {
    List<String> result = WildcardUtils.expandWildcardPattern(pattern, availableFields);
    ImmutableList<String> resultNames = ImmutableList.copyOf(result);
    ImmutableList<String> expected = ImmutableList.copyOf(expectedFields);

    if (resultNames.size() != expected.size() || !resultNames.containsAll(expected)) {
      throw new AssertionError("Expected: " + expected + ", but got: " + resultNames);
    }
  }

  private void testContainsWildcard(String str, boolean expected) {
    boolean result = WildcardUtils.containsWildcard(str);
    if (result != expected) {
      throw new AssertionError("Expected: " + expected + ", but got: " + result);
    }
  }

  @Test
  void testMatchesWildcardPattern() {
    // Test exact match
    testPattern("field", "field", true);
    testPattern("field", "other", false);

    // Test prefix wildcard
    testPattern("account*", "account_number", true);
    testPattern("account*", "firstname", false);

    // Test suffix wildcard
    testPattern("*name", "firstname", true);
    testPattern("*name", "lastname", true);
    testPattern("*name", "balance", false);

    // Test complex pattern
    testPattern("*a*e", "balance", true);
    testPattern("*a*e", "age", true);
    testPattern("*a*e", "city", false);
  }

  @Test
  void testMatchesWildcardPatternEdgeCases() {
    // Test null handling
    assertFalse(WildcardUtils.matchesWildcardPattern(null, "field"));
    assertFalse(WildcardUtils.matchesWildcardPattern("pattern", null));
    assertFalse(WildcardUtils.matchesWildcardPattern(null, null));

    // Test empty strings
    assertTrue(WildcardUtils.matchesWildcardPattern("", ""));
    assertFalse(WildcardUtils.matchesWildcardPattern("", "field"));
    assertFalse(WildcardUtils.matchesWildcardPattern("field", ""));

    // Test single wildcard
    assertTrue(WildcardUtils.matchesWildcardPattern("*", "anything"));
    assertTrue(WildcardUtils.matchesWildcardPattern("*", ""));

    // Test multiple consecutive wildcards
    assertTrue(WildcardUtils.matchesWildcardPattern("**", "field"));
    assertTrue(WildcardUtils.matchesWildcardPattern("a**b", "ab"));
    assertTrue(WildcardUtils.matchesWildcardPattern("a**b", "axxxb"));

    // Test wildcards at start and end
    assertTrue(WildcardUtils.matchesWildcardPattern("*field*", "myfield123"));
    assertTrue(WildcardUtils.matchesWildcardPattern("*field*", "field"));
  }

  @Test
  void testExpandWildcardPattern() {
    // Test exact match
    testExpansion("firstname", ImmutableList.of("firstname"));

    // Test prefix wildcard
    testExpansion("account*", ImmutableList.of("account_number"));

    // Test suffix wildcard
    testExpansion("*name", ImmutableList.of("firstname", "lastname"));

    // Test contains wildcard
    testExpansion(
        "*a*",
        ImmutableList.of("account_number", "firstname", "lastname", "balance", "age", "state"));

    // Test complex pattern
    testExpansion("*a*e", ImmutableList.of("balance", "firstname", "lastname", "age", "state"));

    // Test no matching wildcard
    testExpansion("XYZ*", ImmutableList.of());
  }

  @Test
  void testExpandWildcardPatternEdgeCases() {
    // Test null handling
    assertEquals(List.of(), WildcardUtils.expandWildcardPattern(null, availableFields));
    assertEquals(List.of(), WildcardUtils.expandWildcardPattern("pattern", null));
    assertEquals(List.of(), WildcardUtils.expandWildcardPattern(null, null));

    // Test empty list
    assertEquals(List.of(), WildcardUtils.expandWildcardPattern("*", List.of()));

    // Test single wildcard matches all
    assertEquals(availableFields, WildcardUtils.expandWildcardPattern("*", availableFields));
  }

  @Test
  void testContainsWildcard() {
    // Test with wildcard
    testContainsWildcard("field*", true);
    testContainsWildcard("*field", true);
    testContainsWildcard("*field*", true);

    // Test without wildcard
    testContainsWildcard("field", false);
    testContainsWildcard("", false);
  }

  @Test
  void testContainsWildcardEdgeCases() {
    // Test null
    assertFalse(WildcardUtils.containsWildcard(null));

    // Test multiple wildcards
    assertTrue(WildcardUtils.containsWildcard("**"));
    assertTrue(WildcardUtils.containsWildcard("a*b*c"));
  }

  @Test
  void testConvertWildcardPatternToRegex() {
    // Basic patterns
    assertEquals("^\\Qada\\E$", WildcardUtils.convertWildcardPatternToRegex("ada"));
    assertEquals("^\\Q\\E(.*?)\\Qada\\E$", WildcardUtils.convertWildcardPatternToRegex("*ada"));
    assertEquals("^\\Qada\\E(.*?)\\Q\\E$", WildcardUtils.convertWildcardPatternToRegex("ada*"));
    assertEquals(
        "^\\Q\\E(.*?)\\Qada\\E(.*?)\\Q\\E$", WildcardUtils.convertWildcardPatternToRegex("*ada*"));

    // Multiple wildcards
    assertEquals(
        "^\\Qa\\E(.*?)\\Qb\\E(.*?)\\Qc\\E$", WildcardUtils.convertWildcardPatternToRegex("a*b*c"));

    // Pattern with special regex characters
    assertEquals(
        "^\\Qa.b\\E(.*?)\\Qc+d\\E$", WildcardUtils.convertWildcardPatternToRegex("a.b*c+d"));

    // Single wildcard
    assertEquals("^\\Q\\E(.*?)\\Q\\E$", WildcardUtils.convertWildcardPatternToRegex("*"));

    // Empty pattern
    assertEquals("^\\Q\\E$", WildcardUtils.convertWildcardPatternToRegex(""));

    // Invalid pattern with trailing backslash should throw
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> WildcardUtils.convertWildcardPatternToRegex("pattern\\"));
    assertTrue(ex.getMessage().contains("Invalid escape sequence"));
  }

  @Test
  void testConvertWildcardReplacementToRegex() {
    // No wildcards - literal replacement
    assertEquals("ada", WildcardUtils.convertWildcardReplacementToRegex("ada"));
    assertEquals("test_value", WildcardUtils.convertWildcardReplacementToRegex("test_value"));

    // Single wildcard
    assertEquals("$1", WildcardUtils.convertWildcardReplacementToRegex("*"));

    // Wildcards with text
    assertEquals("$1_$2", WildcardUtils.convertWildcardReplacementToRegex("*_*"));
    assertEquals("prefix_$1", WildcardUtils.convertWildcardReplacementToRegex("prefix_*"));
    assertEquals("$1_suffix", WildcardUtils.convertWildcardReplacementToRegex("*_suffix"));

    // Multiple wildcards
    assertEquals("$1_$2_$3", WildcardUtils.convertWildcardReplacementToRegex("*_*_*"));

    // Empty string
    assertEquals("", WildcardUtils.convertWildcardReplacementToRegex(""));
  }

  @Test
  void testConvertWildcardReplacementToRegexWithEscapes() {
    // Escaped wildcard should be treated as literal
    assertEquals("*", WildcardUtils.convertWildcardReplacementToRegex("\\*")); // \* -> *
    assertEquals("$1_*", WildcardUtils.convertWildcardReplacementToRegex("*_\\*"));
    assertEquals("*_$1", WildcardUtils.convertWildcardReplacementToRegex("\\*_*"));

    // Escaped backslash when there's no wildcard - returned unchanged
    assertEquals("\\\\", WildcardUtils.convertWildcardReplacementToRegex("\\\\"));

    // Mixed escaped and unescaped wildcards
    assertEquals("$1_*_$2", WildcardUtils.convertWildcardReplacementToRegex("*_\\*_*"));
    assertEquals("$1\\$2", WildcardUtils.convertWildcardReplacementToRegex("*\\\\*")); // \\ -> \
  }

  @Test
  void testValidateWildcardSymmetry() {
    // Valid: same number of wildcards
    WildcardUtils.validateWildcardSymmetry("*", "*");
    WildcardUtils.validateWildcardSymmetry("*ada*", "*_*");
    WildcardUtils.validateWildcardSymmetry("a*b*c", "x*y*z");

    // Valid: replacement has no wildcards (literal replacement)
    WildcardUtils.validateWildcardSymmetry("*", "literal");
    WildcardUtils.validateWildcardSymmetry("*ada*", "replacement");
    WildcardUtils.validateWildcardSymmetry("a*b*c", "xyz");

    // Valid: pattern has no wildcards
    WildcardUtils.validateWildcardSymmetry("ada", "replacement");
  }

  @Test
  void testValidateWildcardSymmetryFailure() {
    // Invalid: mismatched wildcard counts
    IllegalArgumentException ex1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> WildcardUtils.validateWildcardSymmetry("*", "**"));
    assertTrue(ex1.getMessage().contains("Wildcard count mismatch"));
    assertTrue(ex1.getMessage().contains("pattern has 1 wildcard(s)"));
    assertTrue(ex1.getMessage().contains("replacement has 2"));

    IllegalArgumentException ex2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> WildcardUtils.validateWildcardSymmetry("*a*b*", "*_*"));
    assertTrue(ex2.getMessage().contains("pattern has 3 wildcard(s)"));
    assertTrue(ex2.getMessage().contains("replacement has 2"));

    IllegalArgumentException ex3 =
        assertThrows(
            IllegalArgumentException.class,
            () -> WildcardUtils.validateWildcardSymmetry("ada", "*"));
    assertTrue(ex3.getMessage().contains("pattern has 0 wildcard(s)"));
    assertTrue(ex3.getMessage().contains("replacement has 1"));
  }

  @Test
  void testValidateWildcardSymmetryWithEscapes() {
    // Escaped wildcards should not count
    WildcardUtils.validateWildcardSymmetry("\\*", "literal"); // 0 wildcards in pattern
    WildcardUtils.validateWildcardSymmetry("*\\*", "*"); // 1 wildcard in both

    // Pattern with 2 wildcards, replacement with 1 wildcard (middle one in \\**\\*)
    WildcardUtils.validateWildcardSymmetry("*", "\\**\\*"); // 1 wildcard in both

    // Should fail when unescaped counts don't match
    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardUtils.validateWildcardSymmetry("*a*", "*\\*")); // 2 vs 1

    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardUtils.validateWildcardSymmetry("*a*", "\\**\\*")); // 2 vs 1
  }
}
