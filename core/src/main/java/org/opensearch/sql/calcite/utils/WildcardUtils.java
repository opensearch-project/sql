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

  /**
   * Check if a field name matches a wildcard pattern.
   *
   * <p>Supports patterns with asterisk (*) wildcards: - "user_*" matches "user_name", "user_id",
   * etc. - "*_id" matches "user_id", "order_id", etc. - "prefix_*_suffix" matches
   * "prefix_middle_suffix"
   *
   * @param pattern The wildcard pattern (may contain *)
   * @param fieldName The field name to match against
   * @return true if the field name matches the pattern
   */
  public static boolean matchesWildcardPattern(String pattern, String fieldName) {
    if (!pattern.contains("*")) {
      return pattern.equals(fieldName);
    }

    // Convert wildcard pattern to regex: split on *, quote literal parts, join with .*
    String[] parts = pattern.split("\\*", -1);
    String regex =
        String.join(
            ".*", java.util.Arrays.stream(parts).map(Pattern::quote).toArray(String[]::new));
    return fieldName.matches(regex);
  }

  /**
   * Expand wildcard pattern to matching field names from available fields.
   *
   * <p>Filters the available fields list to return only those matching the pattern. Maintains the
   * original order of fields from the input list.
   *
   * @param pattern The wildcard pattern (supports * wildcards)
   * @param availableFields List of available field names to filter
   * @return List of field names that match the pattern, in original order
   */
  public static List<String> expandWildcardPattern(String pattern, List<String> availableFields) {
    return availableFields.stream()
        .filter(field -> matchesWildcardPattern(pattern, field))
        .collect(Collectors.toList());
  }
}
