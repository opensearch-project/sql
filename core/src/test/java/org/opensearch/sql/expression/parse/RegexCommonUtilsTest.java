/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.error.ErrorReport;

public class RegexCommonUtilsTest {

  @Test
  public void testGetCompiledPattern() {
    String regex = "test.*pattern";
    Pattern pattern1 = RegexCommonUtils.getCompiledPattern(regex);
    Pattern pattern2 = RegexCommonUtils.getCompiledPattern(regex);

    assertNotNull(pattern1);
    assertSame(pattern1, pattern2, "Should return cached pattern");
    assertEquals(regex, pattern1.pattern());
  }

  @Test
  public void testGetCompiledPatternWithInvalidRegex() {
    String invalidRegex = "[invalid";

    assertThrows(
        PatternSyntaxException.class,
        () -> {
          RegexCommonUtils.getCompiledPattern(invalidRegex);
        });
  }

  @Test
  public void testGetNamedGroupCandidatesSingle() {
    String pattern = "(?<name>[a-z]+)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(1, groups.size());
    assertEquals("name", groups.get(0));
  }

  @Test
  public void testGetNamedGroupCandidatesMultiple() {
    String pattern = "(?<first>[a-z]+)\\s+(?<second>[0-9]+)\\s+(?<third>.*)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(3, groups.size());
    assertEquals("first", groups.get(0));
    assertEquals("second", groups.get(1));
    assertEquals("third", groups.get(2));
  }

  @Test
  public void testGetNamedGroupCandidatesWithMixedGroups() {
    String pattern = "([a-z]+)\\s+(?<named1>[0-9]+)\\s+(\\d+)\\s+(?<named2>.*)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(2, groups.size());
    assertEquals("named1", groups.get(0));
    assertEquals("named2", groups.get(1));
  }

  @Test
  public void testGetNamedGroupCandidatesNoGroups() {
    String pattern = "[a-z]+\\s+[0-9]+";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(0, groups.size());
  }

  @Test
  public void testGetNamedGroupCandidatesEmailPattern() {
    String pattern = ".+@(?<host>.+)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(1, groups.size());
    assertEquals("host", groups.get(0));
  }

  @Test
  public void testMatchesPartialWithMatch() {
    assertTrue(RegexCommonUtils.matchesPartial("test string", "test"));
    assertTrue(RegexCommonUtils.matchesPartial("test string", "string"));
    assertTrue(RegexCommonUtils.matchesPartial("test string", "st.*ng"));
    assertTrue(RegexCommonUtils.matchesPartial("user@domain.com", ".*@domain\\.com"));
  }

  @Test
  public void testMatchesPartialWithoutMatch() {
    assertFalse(RegexCommonUtils.matchesPartial("test string", "notfound"));
    assertFalse(RegexCommonUtils.matchesPartial("test string", "^string"));
    assertFalse(RegexCommonUtils.matchesPartial("user@domain.com", ".*@other\\.com"));
  }

  @Test
  public void testMatchesPartialWithNullInputs() {
    assertFalse(RegexCommonUtils.matchesPartial(null, "pattern"));
    assertFalse(RegexCommonUtils.matchesPartial("text", null));
    assertFalse(RegexCommonUtils.matchesPartial(null, null));
  }

  @Test
  public void testMatchesPartialWithEmptyString() {
    assertTrue(RegexCommonUtils.matchesPartial("", ""));
    assertTrue(RegexCommonUtils.matchesPartial("text", ""));
    assertFalse(RegexCommonUtils.matchesPartial("", "pattern"));
  }

  @Test
  public void testMatchesPartialWithInvalidRegex() {
    assertThrows(
        PatternSyntaxException.class,
        () -> {
          RegexCommonUtils.matchesPartial("text", "[invalid");
        });
  }

  @Test
  public void testExtractNamedGroupSuccess() {
    Pattern pattern = Pattern.compile("(?<user>[^@]+)@(?<domain>.+)");
    String text = "john@example.com";

    assertEquals("john", RegexCommonUtils.extractNamedGroup(text, pattern, "user"));
    assertEquals("example.com", RegexCommonUtils.extractNamedGroup(text, pattern, "domain"));
  }

  @Test
  public void testExtractNamedGroupNoMatch() {
    Pattern pattern = Pattern.compile("(?<user>[^@]+)@(?<domain>.+)");
    String text = "not_an_email";

    assertNull(RegexCommonUtils.extractNamedGroup(text, pattern, "user"));
    assertNull(RegexCommonUtils.extractNamedGroup(text, pattern, "domain"));
  }

  @Test
  public void testExtractNamedGroupNonExistentGroup() {
    Pattern pattern = Pattern.compile("(?<user>[^@]+)@(?<domain>.+)");
    String text = "john@example.com";

    assertNull(RegexCommonUtils.extractNamedGroup(text, pattern, "nonexistent"));
  }

  @Test
  public void testExtractNamedGroupWithNullInputs() {
    Pattern pattern = Pattern.compile("(?<user>[^@]+)@(?<domain>.+)");
    String text = "john@example.com";

    assertNull(RegexCommonUtils.extractNamedGroup(null, pattern, "user"));
    assertNull(RegexCommonUtils.extractNamedGroup(text, null, "user"));
    assertNull(RegexCommonUtils.extractNamedGroup(text, pattern, null));
    assertNull(RegexCommonUtils.extractNamedGroup(null, null, null));
  }

  @Test
  public void testExtractNamedGroupComplexPattern() {
    Pattern pattern = Pattern.compile("(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})");
    String text = "2024-03-15";

    assertEquals("2024", RegexCommonUtils.extractNamedGroup(text, pattern, "year"));
    assertEquals("03", RegexCommonUtils.extractNamedGroup(text, pattern, "month"));
    assertEquals("15", RegexCommonUtils.extractNamedGroup(text, pattern, "day"));
  }

  @Test
  public void testPatternCachingBehavior() {
    String regex1 = "test1.*";
    String regex2 = "test2.*";

    Pattern p1a = RegexCommonUtils.getCompiledPattern(regex1);
    Pattern p2a = RegexCommonUtils.getCompiledPattern(regex2);
    Pattern p1b = RegexCommonUtils.getCompiledPattern(regex1);
    Pattern p2b = RegexCommonUtils.getCompiledPattern(regex2);

    assertSame(p1a, p1b, "Same regex should return cached pattern");
    assertSame(p2a, p2b, "Same regex should return cached pattern");
    assertNotSame(p1a, p2a, "Different regex should return different patterns");
  }

  @Test
  public void testGetNamedGroupCandidatesWithNumericNames() {
    String pattern = "(?<group1>[a-z]+)\\s+(?<group2>[0-9]+)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(2, groups.size());
    assertEquals("group1", groups.get(0));
    assertEquals("group2", groups.get(1));
  }

  @Test
  public void testGetNamedGroupCandidatesWithInvalidCharactersThrowsException() {
    // Test that groups with invalid characters throw exception (even if some are valid)
    String pattern = "(?<validgroup>[a-z]+)\\s+(?<123invalid>[0-9]+)\\s+(?<also-invalid>.*)";
    ErrorReport exception =
        assertThrows(ErrorReport.class, () -> RegexCommonUtils.getNamedGroupCandidates(pattern));
    // Should fail on the first invalid group name found
    assertTrue(exception.getMessage().contains("Invalid capture group name"));
  }

  @Test
  public void testGetNamedGroupCandidatesValidAlphanumeric() {
    String pattern = "(?<groupA>[a-z]+)\\s+(?<group2B>[0-9]+)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(pattern);

    assertEquals(2, groups.size());
    assertEquals("groupA", groups.get(0));
    assertEquals("group2B", groups.get(1));
  }

  @Test
  public void testGetNamedGroupCandidatesWithUnderscore() {
    // Test that underscores in named groups throw ErrorReport
    String patternWithUnderscore = ".+@(?<domain_name>.+)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithUnderscore));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain_name'"));
    assertTrue(exception.getSuggestion().contains("must be alphanumeric"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithHyphen() {
    // Test that hyphens in named groups throw ErrorReport
    String patternWithHyphen = ".+@(?<domain-name>.+)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class, () -> RegexCommonUtils.getNamedGroupCandidates(patternWithHyphen));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain-name'"));
    assertTrue(exception.getSuggestion().contains("must be alphanumeric"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithDot() {
    // Test that dots in named groups throw ErrorReport
    String patternWithDot = ".+@(?<domain.name>.+)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class, () -> RegexCommonUtils.getNamedGroupCandidates(patternWithDot));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain.name'"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithSpace() {
    // Test that spaces in named groups throw ErrorReport
    String patternWithSpace = ".+@(?<domain name>.+)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class, () -> RegexCommonUtils.getNamedGroupCandidates(patternWithSpace));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain name'"));
  }

  @Test
  public void testGetNamedGroupCandidatesStartingWithDigit() {
    // Test that group names starting with digit throw ErrorReport
    String patternStartingWithDigit = ".+@(?<1domain>.+)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternStartingWithDigit));
    assertTrue(exception.getMessage().contains("Invalid capture group name '1domain'"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithSpecialCharacters() {
    // Test that special characters in named groups throw ErrorReport
    String patternWithSpecialChar = ".+@(?<domain@name>.+)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithSpecialChar));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain@name'"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithValidCamelCase() {
    // Test that valid camelCase group names work correctly
    String validPattern = "(?<userName>\\w+)@(?<domainName>\\w+)";
    List<String> groups = RegexCommonUtils.getNamedGroupCandidates(validPattern);
    assertEquals(2, groups.size());
    assertEquals("userName", groups.get(0));
    assertEquals("domainName", groups.get(1));
  }

  @Test
  public void testGetNamedGroupCandidatesWithMixedInvalidValid() {
    // Test that even one invalid group name fails the entire validation
    String patternWithMixed =
        "(?<validName>[a-z]+)\\s+(?<invalid_name>[0-9]+)\\s+(?<anotherValidName>.*)";
    ErrorReport exception =
        assertThrows(
            ErrorReport.class, () -> RegexCommonUtils.getNamedGroupCandidates(patternWithMixed));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'invalid_name'"));
  }

  // --- ReDoS guard (catastrophic backtracking) ---------------------------------------------------

  /** The reporter's PoC pattern: requires 8 'a'-anchored segments. */
  private static final String CATASTROPHIC_PATTERN = "(?<g>(.*a){8})";

  /** Input that can never fully match the pattern, forcing maximal backtracking. */
  private static String catastrophicInput(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n - 1; i++) {
      sb.append('a');
    }
    sb.append('b');
    return sb.toString();
  }

  @Test
  public void extractNamedGroupAbortsCatastrophicBacktrackingQuickly() {
    Pattern pattern = RegexCommonUtils.getCompiledPattern(CATASTROPHIC_PATTERN);
    // Without the bound this input would pin a core for tens of seconds. With the bound it must
    // fail fast. The 5s timeout is generous: the guard trips in milliseconds.
    String input = catastrophicInput(60);
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () -> RegexCommonUtils.extractNamedGroup(input, pattern, "g")));
  }

  @Test
  public void matchesPartialAbortsCatastrophicBacktrackingQuickly() {
    // find() semantics: (.*a){8}b against a long run of 'a' (no trailing 'b') forces catastrophic
    // backtracking exploring every way to place the 8 'a'-anchors. (This nested-.* form still blows
    // up on modern JDKs that optimize the simpler (a+)+$ family.) Without the bound this pins a
    // core for ~12s on 40 chars; with it, it must fail fast.
    String evilPattern = "(.*a){8}b";
    String input = "a".repeat(40);
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () -> RegexCommonUtils.matchesPartial(input, evilPattern)));
  }

  @Test
  public void boundExceededErrorMessageIsClientFriendly() {
    Pattern pattern = RegexCommonUtils.getCompiledPattern(CATASTROPHIC_PATTERN);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.extractNamedGroup(catastrophicInput(60), pattern, "g"));
    assertTrue(e.getMessage().contains("too many characters"));
    assertTrue(e.getMessage().contains("backtracking"));
  }

  @Test
  public void legitimatePatternsStillMatchAfterGuard() {
    // The guard must be transparent to ordinary parse/rex usage.
    Pattern email = RegexCommonUtils.getCompiledPattern("(?<user>[^@]+)@(?<domain>.+)");
    assertEquals("john", RegexCommonUtils.extractNamedGroup("john@example.com", email, "user"));
    assertEquals(
        "example.com", RegexCommonUtils.extractNamedGroup("john@example.com", email, "domain"));
    assertTrue(RegexCommonUtils.matchesPartial("user@domain.com", ".*@domain\\.com"));
    assertFalse(RegexCommonUtils.matchesPartial("test string", "notfound"));
  }

  @Test
  public void legitimateLongInputDoesNotTripGuard() {
    // A 50k-char input matched by a linear pattern reads each char a small constant number of
    // times, far under the limit factor, so it must not trip.
    Pattern pattern = RegexCommonUtils.getCompiledPattern("(?<all>.*)");
    String longInput = "x".repeat(50_000);
    assertEquals(longInput, RegexCommonUtils.extractNamedGroup(longInput, pattern, "all"));
  }
}
