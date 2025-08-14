/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashSet;
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
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("*name"));
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("prefix*"));
    assertFalse(WildcardRenameUtils.isFullWildcardPattern("name"));
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
    LinkedHashSet<String> fields = new LinkedHashSet<>();
    fields.add("firstname");
    fields.add("lastname");
    fields.add("age");
    fields.add("address");
    fields.add("fullname");

    List<String> nameFields = WildcardRenameUtils.matchFieldNames("*name", fields);
    assertEquals(List.of("firstname", "lastname", "fullname"), nameFields);
    List<String> firstFields = WildcardRenameUtils.matchFieldNames("first*", fields);
    assertEquals(List.of("firstname"), firstFields);

    // Test full wildcard - matches all fields
    List<String> allFields = WildcardRenameUtils.matchFieldNames("*", fields);
    assertEquals(List.of("firstname", "lastname", "age", "address", "fullname"), allFields);

    // Test no matches
    List<String> noMatch = WildcardRenameUtils.matchFieldNames("*xyz", fields);
    assertTrue(noMatch.isEmpty());

    // Test exact match (no wildcards)
    List<String> exactMatch = WildcardRenameUtils.matchFieldNames("age", fields);
    assertEquals(List.of("age"), exactMatch);
    List<String> exactNoMatch = WildcardRenameUtils.matchFieldNames("xyz", fields);
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

    // Test full wildcard transformations
    assertEquals(
        "firstname", WildcardRenameUtils.applyWildcardTransformation("*", "*", "firstname"));
    assertEquals(
        "new_firstname",
        WildcardRenameUtils.applyWildcardTransformation("*", "new_*", "firstname"));
    assertEquals(
        "first", WildcardRenameUtils.applyWildcardTransformation("*name", "*", "firstname"));
  }

  @Test
  void testApplyWildcardTransformationErrors() {
    // Test pattern mismatch - field doesn't match source pattern
    assertThrows(
        IllegalArgumentException.class,
        () -> WildcardRenameUtils.applyWildcardTransformation("*name", "*NAME", "age"));
  }

  @Test
  void testValidatePatternCompatibility() {
    // Valid patterns
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*name", "*NAME"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*_*", "*_*"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("prefix*suffix", "PREFIX*SUFFIX"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("name", "NAME"));

    // Valid full wildcard patterns
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*", "*"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*", "new_*"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("*", "*_old"));
    assertTrue(WildcardRenameUtils.validatePatternCompatibility("old_*", "*"));

    // Invalid patterns - mismatched wildcard counts
    assertFalse(WildcardRenameUtils.validatePatternCompatibility("*name", "*_*"));
    assertFalse(WildcardRenameUtils.validatePatternCompatibility("*_*", "*"));
  }
}
