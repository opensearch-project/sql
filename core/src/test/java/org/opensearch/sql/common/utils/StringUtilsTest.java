/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.common.utils.StringUtils.unquoteText;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StringUtilsTest {
  @Test
  void unquoteTest() {
    assertEquals("test", unquoteText("test"));
    assertEquals("test", unquoteText("'test'"));

    assertEquals("test'", unquoteText("'test'''"));
    assertEquals("test\"", unquoteText("\"test\"\"\""));

    assertEquals("te``st", unquoteText("'te``st'"));
    assertEquals("te``st", unquoteText("\"te``st\""));

    assertEquals("te'st", unquoteText("'te''st'"));
    assertEquals("te''st", unquoteText("\"te''st\""));

    assertEquals("te\"\"st", unquoteText("'te\"\"st'"));
    assertEquals("te\"st", unquoteText("\"te\"\"st\""));

    assertEquals("''", unquoteText("''''''"));
    assertEquals("\"\"", unquoteText("\"\"\"\"\"\""));

    assertEquals("test'", unquoteText("'test''"));

    assertEquals("", unquoteText(""));
    assertEquals("'", unquoteText("'"));
    assertEquals("\"", unquoteText("\""));

    assertEquals("hello'", unquoteText("'hello''"));
    assertEquals("don't", unquoteText("'don't'"));
    assertEquals("don\"t", unquoteText("\"don\"t\""));

    assertEquals("hel\\lo'", unquoteText("'hel\\lo''"));
    assertEquals("hel'lo", unquoteText("'hel'lo'"));
    assertEquals("hel\"lo", unquoteText("\"hel\"lo\""));
    assertEquals("hel\\'\\lo", unquoteText("'hel\\\\''\\\\lo'"));
  }

  @Test
  void levenshteinDistanceTest() {
    assertEquals(0, StringUtils.levenshteinDistance("test", "test"));
    assertEquals(1, StringUtils.levenshteinDistance("test", "text"));
    assertEquals(1, StringUtils.levenshteinDistance("test", "tst"));
    assertEquals(3, StringUtils.levenshteinDistance("kitten", "sitting"));
    assertEquals(4, StringUtils.levenshteinDistance("hello", "world"));
    assertEquals(4, StringUtils.levenshteinDistance("test", ""));
    assertEquals(4, StringUtils.levenshteinDistance("", "test"));
    assertEquals(0, StringUtils.levenshteinDistance("", ""));
  }

  @Test
  void findClosestMatchTest() {
    List<String> fields = List.of("name", "age", "email", "address", "phone");

    // Exact match or close typo
    Optional<String> match = StringUtils.findClosestMatch("nam", fields);
    assertTrue(match.isPresent());
    assertEquals("name", match.get());

    match = StringUtils.findClosestMatch("emal", fields);
    assertTrue(match.isPresent());
    assertEquals("email", match.get());

    match = StringUtils.findClosestMatch("addres", fields);
    assertTrue(match.isPresent());
    assertEquals("address", match.get());

    // Case insensitive
    match = StringUtils.findClosestMatch("NAME", fields);
    assertTrue(match.isPresent());
    assertEquals("name", match.get());

    // Too far off - should not match (longer string with many edits)
    match = StringUtils.findClosestMatch("xyzabc", fields);
    assertTrue(match.isEmpty());

    // Empty candidates
    match = StringUtils.findClosestMatch("test", List.of());
    assertTrue(match.isEmpty());
  }
}
