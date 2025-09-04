/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

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
  void testContainsWildcard() {
    // Test with wildcard
    testContainsWildcard("field*", true);
    testContainsWildcard("*field", true);
    testContainsWildcard("*field*", true);

    // Test without wildcard
    testContainsWildcard("field", false);
    testContainsWildcard("", false);
  }
}
