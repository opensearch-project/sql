/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

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
}
