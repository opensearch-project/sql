/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utility class for handling wildcard patterns in rename operations. */
public class WildcardRenameUtils {

  /**
   * Check if pattern contains any supported wildcards.
   *
   * @param pattern the pattern to check
   * @return true if pattern contains * wildcards
   */
  public static boolean isWildcardPattern(String pattern) {
    return pattern.contains("*");
  }

  /**
   * Check if pattern is only wildcards that matches all fields.
   *
   * @param pattern the pattern to check
   * @return true if pattern is only made up of wildcards "*"
   */
  public static boolean isFullWildcardPattern(String pattern) {
    return pattern.matches("\\*+");
  }

  /**
   * Convert wildcard pattern to regex.
   *
   * @param pattern the wildcard pattern
   * @return regex pattern with capture groups
   */
  public static String wildcardToRegex(String pattern) {
    String[] parts = pattern.split("\\*", -1);
    return Arrays.stream(parts).map(Pattern::quote).collect(Collectors.joining("(.*)"));
  }

  /**
   * Match field names against wildcard pattern.
   *
   * @param wildcardPattern the pattern to match against
   * @param availableFields collection of available field names
   * @return list of matching field names
   */
  public static List<String> matchFieldNames(
      String wildcardPattern, Collection<String> availableFields) {
    // Single wildcard matches all available fields
    if (isFullWildcardPattern(wildcardPattern)) {
      return new ArrayList<>(availableFields);
    }

    String regexPattern = "^" + wildcardToRegex(wildcardPattern) + "$";
    Pattern pattern = Pattern.compile(regexPattern);

    return availableFields.stream()
        .filter(field -> pattern.matcher(field).matches())
        .collect(Collectors.toList());
  }

  /**
   * Apply wildcard transformation to get new field name.
   *
   * @param sourcePattern the source wildcard pattern
   * @param targetPattern the target wildcard pattern
   * @param actualFieldName the actual field name to transform
   * @return transformed field name
   * @throws IllegalArgumentException if patterns don't match or are invalid
   */
  public static String applyWildcardTransformation(
      String sourcePattern, String targetPattern, String actualFieldName) {

    if (sourcePattern.equals(targetPattern)) {
      return actualFieldName;
    }

    if (!isFullWildcardPattern(sourcePattern) || !isFullWildcardPattern(targetPattern)) {
      if (sourcePattern.matches(".*\\*{2,}.*") || targetPattern.matches(".*\\*{2,}.*")) {
        throw new IllegalArgumentException("Consecutive wildcards in pattern are not supported");
      }
    }

    String sourceRegex = "^" + wildcardToRegex(sourcePattern) + "$";
    Matcher matcher = Pattern.compile(sourceRegex).matcher(actualFieldName);

    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format("Field '%s' does not match pattern '%s'", actualFieldName, sourcePattern));
    }

    String result = targetPattern;

    for (int i = 1; i <= matcher.groupCount(); i++) {
      String capturedValue = matcher.group(i);

      int index = result.indexOf("*");
      if (index >= 0) {
        result = result.substring(0, index) + capturedValue + result.substring(index + 1);
      } else {
        throw new IllegalArgumentException(
            "Target pattern has fewer wildcards than source pattern");
      }
    }

    return result;
  }

  /**
   * Validate that source and target patterns have matching wildcard counts.
   *
   * @param sourcePattern the source pattern
   * @param targetPattern the target pattern
   * @return true if patterns are compatible
   */
  public static boolean validatePatternCompatibility(String sourcePattern, String targetPattern) {
    int sourceWildcards = countWildcards(sourcePattern);
    int targetWildcards = countWildcards(targetPattern);
    return sourceWildcards == targetWildcards;
  }

  /**
   * Count the number of wildcards in a pattern.
   *
   * @param pattern the pattern to analyze
   * @return number of wildcard characters
   */
  private static int countWildcards(String pattern) {
    return (int) pattern.chars().filter(ch -> ch == '*').count();
  }
}
