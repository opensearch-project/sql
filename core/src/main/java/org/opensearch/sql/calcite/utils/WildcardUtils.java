/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Utility class for wildcard pattern matching in field names. */
public class WildcardUtils {

  private static final String WILDCARD = "*";

  /** Matches a field name against a wildcard pattern using '*' as wildcard character. */
  public static boolean matchesWildcardPattern(String pattern, String fieldName) {
    if (pattern == null || fieldName == null) {
      return false;
    }

    if (!containsWildcard(pattern)) {
      return pattern.equals(fieldName);
    }

    String[] compiledPattern = pattern.split("\\" + WILDCARD, -1);
    return matchesCompiledPattern(compiledPattern, fieldName);
  }

  /**
   * Returns all fields that match the wildcard pattern.
   *
   * @param pattern wildcard pattern with '*' characters
   * @param availableFields list of field names to filter
   * @return filtered list of matching field names
   */
  public static List<String> expandWildcardPattern(String pattern, List<String> availableFields) {
    if (pattern == null || availableFields == null) {
      return List.of();
    }

    if (!containsWildcard(pattern)) {
      return availableFields.stream()
          .filter(field -> pattern.equals(field))
          .collect(Collectors.toList());
    }

    String[] compiledPattern = pattern.split("\\" + WILDCARD, -1);
    return availableFields.stream()
        .filter(field -> matchesCompiledPattern(compiledPattern, field))
        .collect(Collectors.toList());
  }

  /** Matches field name against pre-compiled pattern parts. */
  private static boolean matchesCompiledPattern(String[] parts, String fieldName) {
    if (fieldName == null) {
      return false;
    }

    int startIndex = 0;

    for (int i = 0; i < parts.length - 1; i++) {
      String part = parts[i];

      if (part.isEmpty()) {
        continue;
      }

      if (i == 0) {
        if (!fieldName.startsWith(part)) {
          return false;
        }
        startIndex = part.length();
      } else {
        int index = fieldName.indexOf(part, startIndex);
        if (index == -1) {
          return false;
        }
        startIndex = index + part.length();
      }
    }

    // Check the last part
    String lastPart = parts[parts.length - 1];
    if (!lastPart.isEmpty() && !fieldName.endsWith(lastPart)) {
      return false;
    }

    return true;
  }

  public static boolean containsWildcard(String str) {
    return str != null && str.contains(WILDCARD);
  }

  /**
   * Converts a wildcard pattern to a regex pattern.
   *
   * <p>Example: "*ada" → "^(.*?)ada$"
   *
   * @param wildcardPattern wildcard pattern with '*' and escape sequences (\*, \\)
   * @return regex pattern with capture groups
   */
  public static String convertWildcardPatternToRegex(String wildcardPattern) {
    String[] parts = splitWildcards(wildcardPattern);
    StringBuilder regexBuilder = new StringBuilder("^");

    for (int i = 0; i < parts.length; i++) {
      regexBuilder.append(java.util.regex.Pattern.quote(parts[i]));
      if (i < parts.length - 1) {
        regexBuilder.append("(.*?)"); // Non-greedy capture group for wildcard
      }
    }
    regexBuilder.append("$");

    return regexBuilder.toString();
  }

  /**
   * Converts a wildcard replacement string to a regex replacement string.
   *
   * <p>Example: "*_*" → "$1_$2"
   *
   * @param wildcardReplacement replacement string with '*' and escape sequences (\*, \\)
   * @return regex replacement string with capture group references
   */
  public static String convertWildcardReplacementToRegex(String wildcardReplacement) {
    if (!wildcardReplacement.contains("*")) {
      return wildcardReplacement; // No wildcards = literal replacement
    }

    StringBuilder result = new StringBuilder();
    int captureIndex = 1; // Regex capture groups start at $1
    boolean escaped = false;

    for (char c : wildcardReplacement.toCharArray()) {
      if (escaped) {
        // Handle escape sequences: \* or \\
        result.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '*') {
        // Replace wildcard with $1, $2, etc.
        result.append('$').append(captureIndex++);
      } else {
        result.append(c);
      }
    }

    return result.toString();
  }

  /**
   * Splits a wildcard pattern into parts separated by unescaped wildcards.
   *
   * <p>Example: "a*b*c" → ["a", "b", "c"]
   *
   * @param pattern wildcard pattern with escape sequences
   * @return array of pattern parts
   */
  private static String[] splitWildcards(String pattern) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean escaped = false;

    for (char c : pattern.toCharArray()) {
      if (escaped) {
        current.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '*') {
        parts.add(current.toString());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }

    if (escaped) {
      throw new IllegalArgumentException(
          "Invalid escape sequence: pattern ends with unescaped backslash");
    }

    parts.add(current.toString());
    return parts.toArray(new String[0]);
  }

  /**
   * Counts the number of unescaped wildcards in a string.
   *
   * @param str string to count wildcards in
   * @return number of unescaped wildcards
   */
  private static int countWildcards(String str) {
    int count = 0;
    boolean escaped = false;
    for (char c : str.toCharArray()) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '*') {
        count++;
      }
    }
    return count;
  }

  /**
   * Validates that wildcard count is symmetric between pattern and replacement.
   *
   * <p>Replacement must have either the same number of wildcards as the pattern, or zero wildcards.
   *
   * @param pattern wildcard pattern
   * @param replacement wildcard replacement
   * @throws IllegalArgumentException if wildcard counts are mismatched
   */
  public static void validateWildcardSymmetry(String pattern, String replacement) {
    int patternWildcards = countWildcards(pattern);
    int replacementWildcards = countWildcards(replacement);

    if (replacementWildcards != 0 && replacementWildcards != patternWildcards) {
      throw new IllegalArgumentException(
          String.format(
              "Error in 'replace' command: Wildcard count mismatch - pattern has %d wildcard(s), "
                  + "replacement has %d. Replacement must have same number of wildcards or none.",
              patternWildcards, replacementWildcards));
    }
  }
}
