/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for handling wildcard patterns in rename operations.
 * Supports shell-style (*) wildcards.
 */
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
   * Check if pattern is a single wildcard that matches all fields.
   *
   * @param pattern the pattern to check
   * @return true if pattern is exactly "*"
   */
  public static boolean isFullWildcardPattern(String pattern) {
    return "*".equals(pattern);
  }
  
  
  /**
   * Convert wildcard pattern to regex with capture groups.
   *
   * @param pattern the wildcard pattern
   * @return regex pattern with capture groups
   */
  public static String wildcardToRegex(String pattern) {
    String[] parts = pattern.split("\\*", -1);
    return Arrays.stream(parts)
        .map(Pattern::quote)
        .collect(Collectors.joining("(.*)"));
  }
  
  /**
   * Match field names against wildcard pattern.
   *
   * @param wildcardPattern the pattern to match against
   * @param availableFields set of available field names
   * @return list of matching field names, sorted
   */
  public static List<String> matchFieldNames(String wildcardPattern, Set<String> availableFields) {
    if (!isWildcardPattern(wildcardPattern)) {
      // No wildcards
      return availableFields.contains(wildcardPattern) 
          ? List.of(wildcardPattern) 
          : List.of();
    }
    
    if (isFullWildcardPattern(wildcardPattern)) {
      // Single wildcard matches all available fields
      return availableFields.stream()
          .sorted()
          .collect(Collectors.toList());
    }
    
    String regexPattern = "^" + wildcardToRegex(wildcardPattern) + "$";
    Pattern pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
    
    return availableFields.stream()
        .filter(field -> pattern.matcher(field).matches())
        .sorted()
        .collect(Collectors.toList());
  }
  
  /**
   * Apply wildcard transformation to generate new field name.
   *
   * @param sourcePattern the source wildcard pattern
   * @param targetPattern the target wildcard pattern
   * @param actualFieldName the actual field name to transform
   * @return transformed field name
   * @throws IllegalArgumentException if patterns don't match or are invalid
   */
  public static String applyWildcardTransformation(
      String sourcePattern, 
      String targetPattern, 
      String actualFieldName) {
    
    // No wildcards in either pattern
    if (!isWildcardPattern(sourcePattern) && !isWildcardPattern(targetPattern)) {
      return targetPattern;
    }
    
    // Both are full wildcards
    if (isFullWildcardPattern(sourcePattern) && isFullWildcardPattern(targetPattern)) {
      return actualFieldName;
    }

    if (isFullWildcardPattern(sourcePattern)) {
      // Replace * in target with the actual field name
      return targetPattern.replace("*", actualFieldName);
    }

    String sourceRegex = "^" + wildcardToRegex(sourcePattern) + "$";
    Pattern sourceP = Pattern.compile(sourceRegex, Pattern.CASE_INSENSITIVE);
    Matcher matcher = sourceP.matcher(actualFieldName);
    
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format("Field '%s' does not match pattern '%s'", 
              actualFieldName, sourcePattern));
    }

    String result = targetPattern;

    for (int i = 1; i <= matcher.groupCount(); i++) {
      String capturedValue = matcher.group(i);
      
      int index = result.indexOf("*");
      if (index >= 0) {
        result = result.substring(0, index) + capturedValue + result.substring(index + 1);
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