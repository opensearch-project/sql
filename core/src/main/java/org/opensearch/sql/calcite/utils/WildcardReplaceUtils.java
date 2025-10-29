/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.List;

/** Utility for wildcard-based string replacement in PPL replace command. */
public class WildcardReplaceUtils {

  /** Perform wildcard-based replacement. */
  public static String replaceWithWildcard(String input, String pattern, String replacement) {
    if (input == null) {
      return null;
    }

    if (!pattern.contains("*")) {
      return input.replace(pattern, replacement);
    }

    List<String> captures = matchAndCapture(input, pattern);
    if (captures == null) {
      return input;
    }

    return substituteWildcards(replacement, captures);
  }

  /** Match pattern against input and capture wildcard portions. */
  public static List<String> matchAndCapture(String input, String pattern) {
    List<String> captures = new ArrayList<>();
    String[] parts = pattern.split("\\*", -1); // -1 keeps trailing empty strings

    int inputIndex = 0;

    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];

      if (i == 0) {
        if (!input.startsWith(part)) {
          return null;
        }
        inputIndex = part.length();
      } else if (i == parts.length - 1) {
        if (!input.endsWith(part)) {
          return null;
        }
        int endIndex = input.length() - part.length();
        if (endIndex < inputIndex) {
          return null; // Parts overlap
        }
        captures.add(input.substring(inputIndex, endIndex));
      } else {
        int nextIndex = input.indexOf(part, inputIndex);
        if (nextIndex == -1) {
          return null;
        }
        captures.add(input.substring(inputIndex, nextIndex));
        inputIndex = nextIndex + part.length();
      }
    }

    return captures;
  }

  /** Substitute wildcards in replacement string with captured values. */
  public static String substituteWildcards(String replacement, List<String> captures) {
    if (!replacement.contains("*")) {
      return replacement;
    }

    StringBuilder result = new StringBuilder();
    int captureIndex = 0;

    for (char c : replacement.toCharArray()) {
      if (c == '*') {
        if (captureIndex < captures.size()) {
          result.append(captures.get(captureIndex));
          captureIndex++;
        }
      } else {
        result.append(c);
      }
    }

    return result.toString();
  }

  /** Count the number of wildcards in a string. */
  public static int countWildcards(String str) {
    int count = 0;
    for (char c : str.toCharArray()) {
      if (c == '*') {
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
