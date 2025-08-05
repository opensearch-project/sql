/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for wildcard pattern matching in field names.
 *
 * <p>This class provides functionality for expanding wildcard patterns in PPL field names,
 * primarily used in PROJECT operations (fields command). Supports asterisk (*) wildcards for
 * pattern matching against available field names.
 *
 * <p>Usage examples: - user_* matches user_name, user_id, user_email - *_id matches user_id,
 * order_id, product_id - prefix_*_suffix matches prefix_middle_suffix
 */
public class WildcardUtils {

  /** The wildcard character used in patterns. */
  private static final String WILDCARD_CHAR = "*";

  /**
   * Checks if a field name matches a wildcard pattern.
   *
   * <p>Supports patterns with asterisk (*) wildcards that match zero or more characters. If the
   * pattern contains no wildcards, performs exact string matching.
   *
   * <p>Pattern examples: - "user_*" matches "user_name", "user_id", "user_" - "*_id" matches
   * "user_id", "order_id", "_id" - "prefix_*_suffix" matches "prefix_middle_suffix",
   * "prefix__suffix" - "exact" matches only "exact" (no wildcards)
   *
   * @param pattern The wildcard pattern (may contain * wildcards)
   * @param fieldName The field name to test against the pattern
   * @return true if the field name matches the pattern, false otherwise
   */
  public static boolean matchesWildcardPattern(String pattern, String fieldName) {
    // Fast path: if no wildcards, do exact match
    if (!pattern.contains(WILDCARD_CHAR)) {
      return pattern.equals(fieldName);
    }

    // Convert wildcard pattern to regex:
    // 1. Split on * to get literal parts
    // 2. Quote each literal part to escape regex special characters
    // 3. Join with .* (regex for "zero or more characters")
    String[] parts = pattern.split("\\*", -1);
    String regex =
        String.join(
            ".*", java.util.Arrays.stream(parts).map(Pattern::quote).toArray(String[]::new));
    return fieldName.matches(regex);
  }

  /**
   * Expands a wildcard pattern to matching field names from available fields.
   *
   * <p>This method is primarily used in PROJECT operations (fields command) to expand wildcard
   * patterns to concrete field names. Filters the available fields list to return only those
   * matching the pattern, maintaining the original order.
   *
   * <p>Usage example in PPL context: source=table | fields user_*, account_* Expands to:
   * source=table | fields user_name, user_id, account_balance, account_type
   *
   * @param pattern The wildcard pattern (supports * wildcards)
   * @param availableFields List of available field names to filter against
   * @return List of field names that match the pattern, preserving input order. Returns empty list
   *     if no matches found.
   */
  public static List<String> expandWildcardPattern(String pattern, List<String> availableFields) {
    // Filter available fields to only those matching the wildcard pattern
    return availableFields.stream()
        .filter(field -> matchesWildcardPattern(pattern, field))
        .collect(Collectors.toList());
  }

  /**
   * Checks if a string contains wildcard characters.
   *
   * <p>This method provides a consistent way to detect wildcard patterns across all components in
   * the codebase. Used by both Calcite and non-Calcite wildcard resolution logic.
   *
   * @param str String to check for wildcard characters
   * @return true if string contains '*' wildcard characters, false otherwise
   */
  public static boolean containsWildcard(String str) {
    return str.contains(WILDCARD_CHAR);
  }
}
