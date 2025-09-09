/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class WildcardRenameUtilsTest {

  @Test
  void testIsWildcardPattern() {
    assertTrue(WildcardRenameUtils.isWildcardPattern("*name"));
    assertTrue(WildcardRenameUtils.isWildcardPattern("prefix*suffix"));
    assertTrue(WildcardRenameUtils.isWildcardPattern("*"));
    assertFalse(WildcardRenameUtils.isWildcardPattern("name"));
    assertFalse(WildcardRenameUtils.isWildcardPattern(""));
  }

  @Test
  void testIsFullWildcardPattern() {
    assertTrue(WildcardRenameUtils.isFullWildcardPattern("*"));
    assertTrue(WildcardRenameUtils.isFullWildcardPattern("**"));
    assertTrue(WildcardRenameUtils.isFullWildcardPattern("***"));
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("*name"));
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("prefix*"));
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("name"));
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("*_*"));
  }

  @Test
  void testWildcardToRegex() {
    assertEquals("\\Q\\E(.*)\\Qname\\E", WildcardRenameUtils.wildcardToRegex("*name"));
    assertEquals("\\Qname\\E(.*)\\Q\\E", WildcardRenameUtils.wildcardToRegex("name*"));
    assertEquals(
        "\\Q\\E(.*)\\Q_\\E(.*)\\Q_field\\E", WildcardRenameUtils.wildcardToRegex("*_*_field"));
  }

  @Test
  void testMatchFieldNames() {
    ImmutableSet<String> availableFields =
        ImmutableSet.of("firstname", "lastname", "age", "address", "fullname");

    List<String> nameFields = WildcardRenameUtils.matchFieldNames("*name", availableFields);
    assertEquals(List.of("firstname", "lastname", "fullname"), nameFields);
    List<String> firstFields = WildcardRenameUtils.matchFieldNames("first*", availableFields);
    assertEquals(List.of("firstname"), firstFields);
    List<String> allFields = WildcardRenameUtils.matchFieldNames("*", availableFields);
    assertEquals(List.of("firstname", "lastname", "age", "address", "fullname"), allFields);
  }

  @Test
  void testMatchFieldNamesNoMatches() {
    ImmutableSet<String> availableFields =
        ImmutableSet.of("firstname", "lastname", "age", "address", "fullname");
    List<String> noMatch = WildcardRenameUtils.matchFieldNames("*xyz", availableFields);
    assertTrue(noMatch.isEmpty());
  }

  @Test
  void testMatchFieldNamesNoWildcards() {
    ImmutableSet<String> availableFields =
        ImmutableSet.of("firstname", "lastname", "age", "address", "fullname");
    List<String> exactMatch = WildcardRenameUtils.matchFieldNames("age", availableFields);
    assertEquals(List.of("age"), exactMatch);
    List<String> exactNoMatch = WildcardRenameUtils.matchFieldNames("xyz", availableFields);
    assertTrue(exactNoMatch.isEmpty());
  }

  @Test
  void testApplyWildcardTransformation() {
    assertEquals(
        "firstNAME",
        WildcardRenameUtils.applyWildcardTransformation("*name", "*NAME", "firstname"));
    assertEquals(
        "FIRSTname",
        WildcardRenameUtils.applyWildcardTransformation("first*", "FIRST*", "firstname"));
    assertEquals(
        "user_profile",
        WildcardRenameUtils.applyWildcardTransformation("*_*_field", "*_*", "user_profile_field"));
    assertEquals(
        "prefixfirst",
        WildcardRenameUtils.applyWildcardTransformation("*name", "prefix*", "firstname"));
  }

  @Test
  void testFullWildcardTransformation() {
    assertEquals(
        "firstname", WildcardRenameUtils.applyWildcardTransformation("*", "*", "firstname"));
    assertEquals(
        "new_firstname",
        WildcardRenameUtils.applyWildcardTransformation("*", "new_*", "firstname"));
    assertEquals(
        "first", WildcardRenameUtils.applyWildcardTransformation("*name", "*", "firstname"));
  }

  @Test
  void testPartialMatchWildcardTransformation() {
    assertEquals(
        "FiRsTname",
        WildcardRenameUtils.applyWildcardTransformation("f*r*tname", "F*R*Tname", "firstname"));
  }

  @Test
  void testApplyWildcardTransformationPatternMismatch() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardRenameUtils.applyWildcardTransformation("*name", "*NAME", "age"));
  }

  @Test
  void testValidatePatternCompatibility() {
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*name", "*NAME"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*_*", "*_*"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("prefix*suffix", "PREFIX*SUFFIX"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("name", "NAME"));
  }

  @Test
  void testValidatePatternCompatibilityFullWildcard() {
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*", "*"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*", "new_*"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*", "*_old"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("old_*", "*"));
  }

  @Test
  void testValidatePatternCompatibilityInvalidPattern() {
    assertFalse(WildcardRenameUtils.validatePatternCompatibility("*name", "*_*"));
    assertFalse(WildcardRenameUtils.validatePatternCompatibility("*_*", "*"));
  }

  @Test
  void testFullWildcardPatternTransformation() {
    assertEquals(
        "firstname", WildcardRenameUtils.applyWildcardTransformation("*", "*", "firstname"));
    assertEquals(
        "firstname", WildcardRenameUtils.applyWildcardTransformation("**", "**", "firstname"));
    assertEquals(
        "firstname", WildcardRenameUtils.applyWildcardTransformation("***", "***", "firstname"));
  }

  @Test
  void testSingleWildcardFullPatternTransformation() {
    assertEquals(
        "firstname_suffix",
        WildcardRenameUtils.applyWildcardTransformation("*", "*_suffix", "firstname"));
    assertEquals(
        "prefix_firstname",
        WildcardRenameUtils.applyWildcardTransformation("*", "prefix_*", "firstname"));
    assertEquals(
        "prefix_firstname_suffix",
        WildcardRenameUtils.applyWildcardTransformation("*", "prefix_*_suffix", "firstname"));
  }

  @Test
  void testMultipleWildcardSourcePatternError() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardRenameUtils.applyWildcardTransformation("**", "**_suffix", "firstname"));
    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardRenameUtils.applyWildcardTransformation("***", "prefix_***", "firstname"));
  }

  @Test
  void testValidatePatternCompatibilityMultipleWildcards() {
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("**", "**"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("***", "***"));
    assertFalse(WildcardRenameUtils.validatePatternCompatibility("**", "*"));
    assertFalse(WildcardRenameUtils.validatePatternCompatibility("*", "**"));
  }

  @Test
  void testConsecutiveWildcardsError() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardRenameUtils.applyWildcardTransformation("*n**me", "*N**ME", "firstname"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WildcardRenameUtils.applyWildcardTransformation("**name", "**something", "firstname"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WildcardRenameUtils.applyWildcardTransformation(
                "**name**", "**something**", "firstname"));
  }
}
