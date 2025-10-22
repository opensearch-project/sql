/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;

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
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(pattern));
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
    // Test that underscores in named groups throw IllegalArgumentException
    String patternWithUnderscore = ".+@(?<domain_name>.+)";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithUnderscore));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain_name'"));
    assertTrue(
        exception
            .getMessage()
            .contains("must start with a letter and contain only letters and digits"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithHyphen() {
    // Test that hyphens in named groups throw IllegalArgumentException
    String patternWithHyphen = ".+@(?<domain-name>.+)";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithHyphen));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain-name'"));
    assertTrue(
        exception
            .getMessage()
            .contains("must start with a letter and contain only letters and digits"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithDot() {
    // Test that dots in named groups throw IllegalArgumentException
    String patternWithDot = ".+@(?<domain.name>.+)";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithDot));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain.name'"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithSpace() {
    // Test that spaces in named groups throw IllegalArgumentException
    String patternWithSpace = ".+@(?<domain name>.+)";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithSpace));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'domain name'"));
  }

  @Test
  public void testGetNamedGroupCandidatesStartingWithDigit() {
    // Test that group names starting with digit throw IllegalArgumentException
    String patternStartingWithDigit = ".+@(?<1domain>.+)";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternStartingWithDigit));
    assertTrue(exception.getMessage().contains("Invalid capture group name '1domain'"));
  }

  @Test
  public void testGetNamedGroupCandidatesWithSpecialCharacters() {
    // Test that special characters in named groups throw IllegalArgumentException
    String patternWithSpecialChar = ".+@(?<domain@name>.+)";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
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
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RegexCommonUtils.getNamedGroupCandidates(patternWithMixed));
    assertTrue(exception.getMessage().contains("Invalid capture group name 'invalid_name'"));
  }
}
