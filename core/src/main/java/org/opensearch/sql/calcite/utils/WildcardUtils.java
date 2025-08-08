/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utility class for wildcard pattern matching in field names. */
public class WildcardUtils {

  private static final String WILDCARD_CHAR = "*";

  public static boolean matchesWildcardPattern(String pattern, String fieldName) {

    if (!pattern.contains(WILDCARD_CHAR)) {
      return pattern.equals(fieldName);
    }

    String[] parts = pattern.split("\\*", -1);
    String regex =
        String.join(
            ".*", java.util.Arrays.stream(parts).map(Pattern::quote).toArray(String[]::new));
    return fieldName.matches(regex);
  }

  public static List<String> expandWildcardPattern(String pattern, List<String> availableFields) {

    return availableFields.stream()
        .filter(field -> matchesWildcardPattern(pattern, field))
        .collect(Collectors.toList());
  }

  public static boolean containsWildcard(String str) {
    return str.contains(WILDCARD_CHAR);
  }
}
