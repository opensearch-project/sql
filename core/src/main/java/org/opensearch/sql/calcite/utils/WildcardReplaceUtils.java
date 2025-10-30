/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility for wildcard-based string replacement in PPL replace command.
 *
 * <p>Supports wildcard patterns using '*' to match zero or more characters. Wildcards in the
 * replacement string are substituted with values captured from the pattern match.
 *
 * <p>Escape sequences: Use '\*' to match literal asterisks and '\\' to match literal backslashes.
 * Without escapes, '*' is interpreted as a wildcard pattern.
 */
public class WildcardReplaceUtils {

  private static final int PATTERN_CACHE_SIZE = 100;

  private static final Map<String, Pattern> PATTERN_CACHE =
      Collections.synchronizedMap(
          new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Pattern> eldest) {
              return size() > PATTERN_CACHE_SIZE;
            }
          });

  /** Perform wildcard-based replacement. */
  public static String replaceWithWildcard(String input, String pattern, String replacement) {
    if (input == null) {
      return null;
    }

    validateEscapeSequences(pattern);
    validateEscapeSequences(replacement);

    if (!pattern.contains("*")) {
      return input.replace(pattern, replacement);
    }

    List<String> captures = matchAndCapture(input, pattern);
    if (captures == null) {
      return input;
    }

    return substituteWildcards(replacement, captures);
  }

  /** Validate that string doesn't end with unescaped backslash. */
  private static void validateEscapeSequences(String str) {
    boolean escaped = false;
    for (char c : str.toCharArray()) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      }
    }
    if (escaped) {
      throw new IllegalArgumentException(
          "Invalid escape sequence: pattern ends with unescaped backslash");
    }
  }

  /** Match pattern against input and capture wildcard portions. */
  public static List<String> matchAndCapture(String input, String pattern) {
    Pattern compiledPattern =
        PATTERN_CACHE.computeIfAbsent(pattern, WildcardReplaceUtils::compileWildcardPattern);

    Matcher matcher = compiledPattern.matcher(input);
    if (!matcher.matches()) {
      return null;
    }

    List<String> captures = new ArrayList<>();
    for (int i = 1; i <= matcher.groupCount(); i++) {
      captures.add(matcher.group(i));
    }
    return captures;
  }

  /**
   * Split pattern on unescaped wildcards, handling escape sequences.
   *
   * <p>Supports: \* (literal asterisk), \\ (literal backslash)
   *
   * @param pattern Wildcard pattern with potential escapes
   * @return Array of literal parts between wildcards
   * @throws IllegalArgumentException if pattern ends with unescaped backslash
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

  /** Compile a wildcard pattern to a regex Pattern. */
  private static Pattern compileWildcardPattern(String pattern) {
    String[] parts = splitWildcards(pattern);
    StringBuilder regexBuilder = new StringBuilder("^");

    for (int i = 0; i < parts.length; i++) {
      regexBuilder.append(Pattern.quote(parts[i]));
      if (i < parts.length - 1) {
        regexBuilder.append("(.*?)");
      }
    }
    regexBuilder.append("$");

    return Pattern.compile(regexBuilder.toString());
  }

  /** Substitute wildcards in replacement string with captured values. */
  public static String substituteWildcards(String replacement, List<String> captures) {
    StringBuilder result = new StringBuilder();
    int captureIndex = 0;
    boolean escaped = false;

    for (char c : replacement.toCharArray()) {
      if (escaped) {
        result.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '*') {
        if (captureIndex < captures.size()) {
          result.append(captures.get(captureIndex));
          captureIndex++;
        }
      } else {
        result.append(c);
      }
    }

    if (escaped) {
      throw new IllegalArgumentException(
          "Invalid escape sequence: replacement ends with unescaped backslash");
    }

    return result.toString();
  }

  /** Count the number of unescaped wildcards in a string. */
  public static int countWildcards(String str) {
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

  /** Validate wildcard symmetry between pattern and replacement. */
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
