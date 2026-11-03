/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class RegexCommonUtilsTest {

  @AfterEach
  public void restoreDefault() {
    RegexCommonUtils.setMatchLimit(RegexCommonUtils.DEFAULT_MATCH_LIMIT);
  }

  @Test
  public void defaultMatchLimitIsSplunkAligned() {
    assertEquals(100_000L, RegexCommonUtils.DEFAULT_MATCH_LIMIT);
    assertEquals(RegexCommonUtils.DEFAULT_MATCH_LIMIT, RegexCommonUtils.getMatchLimit());
  }

  @Test
  public void setMatchLimitChangesTheEffectiveCap() {
    RegexCommonUtils.setMatchLimit(42L);
    assertEquals(42L, RegexCommonUtils.getMatchLimit());
  }

  @Test
  public void setMatchLimitIgnoresNonPositiveValues() {
    RegexCommonUtils.setMatchLimit(500L);
    RegexCommonUtils.setMatchLimit(0L);
    assertEquals(500L, RegexCommonUtils.getMatchLimit(), "zero must not disable the guard");
    RegexCommonUtils.setMatchLimit(-1L);
    assertEquals(500L, RegexCommonUtils.getMatchLimit(), "negative must not disable the guard");
  }

  @Test
  public void boundedMatcherAbortsCatastrophicMatchQuickly() {
    // Classic catastrophic-backtracking pattern over a long non-matching input. Unbounded this
    // pins a core for seconds; bounded it must throw almost immediately.
    Pattern pattern = Pattern.compile("(?<g>(.*a){8})b");
    String input = "a".repeat(60);
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          Matcher matcher = RegexCommonUtils.boundedMatcher(pattern, input);
          IllegalArgumentException e =
              assertThrows(IllegalArgumentException.class, matcher::matches);
          assertTrue(e.getMessage().contains("too many characters"));
        });
  }

  @Test
  public void boundedMatcherLeavesLegitimateMatchesUnaffected() {
    Pattern pattern = Pattern.compile("(?<user>[^@]+)@(?<domain>.+)");
    Matcher matcher = RegexCommonUtils.boundedMatcher(pattern, "amberduke@pyrami.com");
    assertTrue(matcher.matches());
    assertEquals("amberduke", matcher.group("user"));
    assertEquals("pyrami.com", matcher.group("domain"));
  }
}
