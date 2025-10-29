/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.opensearch.sql.calcite.utils.WildcardReplaceUtils;

/** Unit tests for {@link WildcardReplaceUtils}. */
public class WildcardReplaceUtilsTest {

  // ========== Basic Wildcard Matching Tests ==========

  @Test
  public void testWildcardMatch_prefixWildcard() {
    assertEquals(
        "localhost",
        WildcardReplaceUtils.replaceWithWildcard("server.localhost", "*localhost", "localhost"));
  }

  @Test
  public void testWildcardMatch_suffixWildcard() {
    assertEquals(
        "server", WildcardReplaceUtils.replaceWithWildcard("server.local", "server*", "server"));
  }

  @Test
  public void testWildcardMatch_infixWildcard() {
    assertEquals(
        "replaced", WildcardReplaceUtils.replaceWithWildcard("fooXYZbar", "*XYZ*", "replaced"));
  }

  @Test
  public void testWildcardMatch_multipleWildcards() {
    assertEquals("foo_bar", WildcardReplaceUtils.replaceWithWildcard("foo - bar", "* - *", "*_*"));
  }

  @Test
  public void testWildcardMatch_noMatch() {
    String input = "server.example.com";
    assertEquals(input, WildcardReplaceUtils.replaceWithWildcard(input, "*localhost", "localhost"));
  }

  @Test
  public void testWildcardMatch_onlyWildcard() {
    assertEquals("replaced", WildcardReplaceUtils.replaceWithWildcard("anything", "*", "replaced"));
  }

  // ========== Wildcard Capture and Substitution Tests ==========

  @Test
  public void testWildcardCapture_single() {
    assertEquals(
        "localhost server",
        WildcardReplaceUtils.replaceWithWildcard("server localhost", "* localhost", "localhost *"));
  }

  @Test
  public void testWildcardCapture_multiple() {
    // Pattern "* - *" captures ["foo", "bar"], replacement "*_*" gives "foo_bar"
    assertEquals("foo_bar", WildcardReplaceUtils.replaceWithWildcard("foo - bar", "* - *", "*_*"));
  }

  @Test
  public void testWildcardCapture_reorder() {
    assertEquals(
        "localhost server",
        WildcardReplaceUtils.replaceWithWildcard("server localhost", "* localhost", "localhost *"));
  }

  @Test
  public void testWildcardSubstitute_noWildcards() {
    assertEquals("fixed", WildcardReplaceUtils.replaceWithWildcard("foo bar", "* bar", "fixed"));
  }

  @Test
  public void testWildcardSubstitute_moreCapturesThanWildcards() {
    // Pattern: "* - * - *" captures 3 values
    // Replacement: "*_*" uses only 2
    assertEquals(
        "foo_bar", WildcardReplaceUtils.replaceWithWildcard("foo - bar - baz", "* - * - *", "*_*"));
  }

  // ========== Edge Cases ==========

  @Test
  public void testWildcard_emptyCapture() {
    // Wildcard matches empty string
    assertEquals(
        "fixed", WildcardReplaceUtils.replaceWithWildcard("localhost", "*localhost", "fixed"));
  }

  @Test
  public void testWildcard_emptyCaptureWithSubstitution() {
    // Empty capture should be substituted as empty string
    assertEquals(
        "localhost ",
        WildcardReplaceUtils.replaceWithWildcard("localhost", "*localhost", "localhost *"));
  }

  @Test
  public void testWildcard_overlappingParts() {
    // No valid match - parts overlap
    assertNull(WildcardReplaceUtils.matchAndCapture("foo", "foo*foo"));
  }

  @Test
  public void testWildcard_consecutiveWildcards() {
    // "**" treated as two separate wildcards
    // Pattern "**" splits to ["", "", ""], so first wildcard captures empty, second captures all
    List<String> captures = WildcardReplaceUtils.matchAndCapture("foobar", "**");
    assertNotNull(captures);
    assertEquals(2, captures.size());
    // First wildcard captures empty (greedy matching finds "" immediately)
    // Second wildcard captures the rest
    assertEquals("", captures.get(0));
    assertEquals("foobar", captures.get(1));
  }

  @Test
  public void testWildcard_emptyString() {
    // Pattern "*" matches empty string (wildcard matches zero or more chars)
    assertEquals("replacement", WildcardReplaceUtils.replaceWithWildcard("", "*", "replacement"));
  }

  @Test
  public void testWildcard_nullInput() {
    assertNull(WildcardReplaceUtils.replaceWithWildcard(null, "*", "replacement"));
  }

  @Test
  public void testWildcard_singleWildcardMatchesAll() {
    // Pattern "*" contains a wildcard, so it matches the entire input
    String input = "foo * bar";
    assertEquals("replaced", WildcardReplaceUtils.replaceWithWildcard(input, "*", "replaced"));
  }

  // ========== Literal Replacement (No Wildcards) ==========

  @Test
  public void testLiteral_noWildcards() {
    assertEquals("Illinois", WildcardReplaceUtils.replaceWithWildcard("IL", "IL", "Illinois"));
  }

  @Test
  public void testLiteral_multipleOccurrences() {
    assertEquals(
        "Illinois Illinois", WildcardReplaceUtils.replaceWithWildcard("IL IL", "IL", "Illinois"));
  }

  @Test
  public void testLiteral_noMatch() {
    String input = "California";
    assertEquals(input, WildcardReplaceUtils.replaceWithWildcard(input, "IL", "Illinois"));
  }

  // ========== Validation Tests ==========

  @Test
  public void testValidation_symmetryValid_sameCount() {
    // Should not throw exception
    WildcardReplaceUtils.validateWildcardSymmetry("* - *", "*_*");
  }

  @Test
  public void testValidation_symmetryValid_zeroInReplacement() {
    // Should not throw exception
    WildcardReplaceUtils.validateWildcardSymmetry("* - *", "fixed");
  }

  @Test
  public void testValidation_symmetryInvalid_mismatch() {
    try {
      WildcardReplaceUtils.validateWildcardSymmetry("* - *", "*");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException exception) {
      assertTrue(exception.getMessage().contains("Wildcard count mismatch"));
      assertTrue(exception.getMessage().contains("pattern has 2 wildcard(s)"));
      assertTrue(exception.getMessage().contains("replacement has 1"));
    }
  }

  @Test
  public void testValidation_symmetryValid_noWildcardsInPattern() {
    // Should not throw exception
    WildcardReplaceUtils.validateWildcardSymmetry("foo", "bar");
  }

  // ========== Count Wildcards Tests ==========

  @Test
  public void testCountWildcards_none() {
    assertEquals(0, WildcardReplaceUtils.countWildcards("no wildcards here"));
  }

  @Test
  public void testCountWildcards_single() {
    assertEquals(1, WildcardReplaceUtils.countWildcards("*wildcard"));
  }

  @Test
  public void testCountWildcards_multiple() {
    assertEquals(3, WildcardReplaceUtils.countWildcards("* - * - *"));
  }

  @Test
  public void testCountWildcards_consecutive() {
    assertEquals(2, WildcardReplaceUtils.countWildcards("**"));
  }

  // ========== Match and Capture Internal Tests ==========

  @Test
  public void testMatchAndCapture_prefixWildcard() {
    List<String> captures = WildcardReplaceUtils.matchAndCapture("server.localhost", "*localhost");
    assertNotNull(captures);
    assertEquals(1, captures.size());
    assertEquals("server.", captures.get(0));
  }

  @Test
  public void testMatchAndCapture_suffixWildcard() {
    List<String> captures = WildcardReplaceUtils.matchAndCapture("server.local", "server*");
    assertNotNull(captures);
    assertEquals(1, captures.size());
    assertEquals(".local", captures.get(0));
  }

  @Test
  public void testMatchAndCapture_middlePart() {
    List<String> captures = WildcardReplaceUtils.matchAndCapture("foo - bar", "* - *");
    assertNotNull(captures);
    assertEquals(2, captures.size());
    assertEquals("foo", captures.get(0));
    assertEquals("bar", captures.get(1));
  }

  @Test
  public void testMatchAndCapture_noMatch_wrongPrefix() {
    assertNull(WildcardReplaceUtils.matchAndCapture("server.localhost", "client*"));
  }

  @Test
  public void testMatchAndCapture_noMatch_wrongSuffix() {
    assertNull(WildcardReplaceUtils.matchAndCapture("server.localhost", "*example"));
  }

  @Test
  public void testMatchAndCapture_noMatch_missingMiddle() {
    assertNull(WildcardReplaceUtils.matchAndCapture("foo bar", "* - *"));
  }

  // ========== Substitute Wildcards Internal Tests ==========

  @Test
  public void testSubstituteWildcards_single() {
    assertEquals(
        "prefix_foo", WildcardReplaceUtils.substituteWildcards("prefix_*", Arrays.asList("foo")));
  }

  @Test
  public void testSubstituteWildcards_multiple() {
    assertEquals(
        "foo_bar", WildcardReplaceUtils.substituteWildcards("*_*", Arrays.asList("foo", "bar")));
  }

  @Test
  public void testSubstituteWildcards_noWildcardsInReplacement() {
    assertEquals(
        "fixed", WildcardReplaceUtils.substituteWildcards("fixed", Arrays.asList("foo", "bar")));
  }

  @Test
  public void testSubstituteWildcards_moreWildcardsThanCaptures() {
    // Should use available captures, skip extra wildcards
    assertEquals("foo_", WildcardReplaceUtils.substituteWildcards("*_*", Arrays.asList("foo")));
  }

  // ========== SPL Examples from Documentation ==========

  @Test
  public void testSPLExample1_replaceSuffix() {
    // SPL: replace *localhost WITH localhost IN host
    // Input: "server.localhost" → Output: "localhost"
    assertEquals(
        "localhost",
        WildcardReplaceUtils.replaceWithWildcard("server.localhost", "*localhost", "localhost"));
  }

  @Test
  public void testSPLExample2_reorderWithCapture() {
    // SPL: replace "* localhost" WITH "localhost *" IN host
    // Input: "server localhost" → Output: "localhost server"
    assertEquals(
        "localhost server",
        WildcardReplaceUtils.replaceWithWildcard("server localhost", "* localhost", "localhost *"));
  }

  @Test
  public void testSPLExample3_multipleWildcards() {
    // SPL: replace "* - *" WITH "*_*" IN field
    // Input: "foo - bar" → Output: "foo_bar"
    assertEquals("foo_bar", WildcardReplaceUtils.replaceWithWildcard("foo - bar", "* - *", "*_*"));
  }

  @Test
  public void testSPLExample4_infixReplacement() {
    // SPL: replace *XYZ* WITH *ALL* IN _time
    // Input: "fooXYZbar" → Output: "fooALLbar"
    assertEquals(
        "fooALLbar", WildcardReplaceUtils.replaceWithWildcard("fooXYZbar", "*XYZ*", "*ALL*"));
  }
}
