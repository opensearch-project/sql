/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Common utilities for regex operations. Provides pattern caching and consistent matching behavior.
 */
public class RegexCommonUtils {

  private static final Pattern NAMED_GROUP_PATTERN =
      Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

  // Pattern to extract ANY named group (valid or invalid) for validation
  private static final Pattern ANY_NAMED_GROUP_PATTERN = Pattern.compile("\\(\\?<([^>]+)>");

  private static final int MAX_CACHE_SIZE = 1000;

  private static final Map<String, Pattern> patternCache =
      Collections.synchronizedMap(
          new LinkedHashMap<>(MAX_CACHE_SIZE + 1, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Pattern> eldest) {
              return size() > MAX_CACHE_SIZE;
            }
          });

  /**
   * Get compiled pattern from cache or compile and cache it.
   *
   * @param regex The regex pattern string
   * @return Compiled Pattern object
   * @throws PatternSyntaxException if the regex is invalid
   */
  public static Pattern getCompiledPattern(String regex) {
    Pattern pattern = patternCache.get(regex);
    if (pattern == null) {
      pattern = Pattern.compile(regex);
      patternCache.put(regex, pattern);
    }
    return pattern;
  }

  /**
   * Extract list of named group candidates from a regex pattern. Validates that all group names
   * conform to Java regex named group requirements: must start with a letter and contain only
   * letters and digits.
   *
   * @param pattern The regex pattern string
   * @return List of valid named group names found in the pattern
   * @throws IllegalArgumentException if any named groups contain invalid characters
   */
  public static List<String> getNamedGroupCandidates(String pattern) {
    ImmutableList.Builder<String> namedGroups = ImmutableList.builder();

    Matcher anyGroupMatcher = ANY_NAMED_GROUP_PATTERN.matcher(pattern);
    while (anyGroupMatcher.find()) {
      String groupName = anyGroupMatcher.group(1);

      if (!isValidJavaRegexGroupName(groupName)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid capture group name '%s'. Java regex group names must start with a letter"
                    + " and contain only letters and digits.",
                groupName));
      }
    }

    Matcher validGroupMatcher = NAMED_GROUP_PATTERN.matcher(pattern);
    while (validGroupMatcher.find()) {
      namedGroups.add(validGroupMatcher.group(1));
    }

    return namedGroups.build();
  }

  /**
   * Validates if a group name conforms to Java regex named group requirements. Java regex group
   * names must: - Start with a letter (a-z, A-Z) - Contain only letters (a-z, A-Z) and digits (0-9)
   *
   * @param groupName The group name to validate
   * @return true if valid, false otherwise
   */
  private static boolean isValidJavaRegexGroupName(String groupName) {
    if (groupName == null || groupName.isEmpty()) {
      return false;
    }
    return groupName.matches("^[A-Za-z][A-Za-z0-9]*$");
  }

  /**
   * Match using find() for partial match semantics with string pattern.
   *
   * @param text The text to match against
   * @param patternStr The pattern string
   * @return true if pattern is found anywhere in the text
   * @throws PatternSyntaxException if the regex is invalid
   */
  public static boolean matchesPartial(String text, String patternStr) {
    if (text == null || patternStr == null) {
      return false;
    }
    Pattern pattern = getCompiledPattern(patternStr);
    return pattern.matcher(text).find();
  }

  /**
   * Extract a specific named group from text using the pattern. Used by parse command regex method.
   *
   * @param text The text to extract from
   * @param pattern The compiled pattern with named groups
   * @param groupName The name of the group to extract
   * @return The extracted value or null if not found
   */
  public static String extractNamedGroup(String text, Pattern pattern, String groupName) {
    if (text == null || pattern == null || groupName == null) {
      return null;
    }

    Matcher matcher = pattern.matcher(text);

    if (matcher.find()) {
      try {
        return matcher.group(groupName);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }

    return null;
  }
}
