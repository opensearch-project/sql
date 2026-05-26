/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

/** Utility class for query_string syntax operations. */
public class QueryStringUtils {

  private static final String INTERNAL_TIMESTAMP = "@timestamp";

  public static final String MASK_LITERAL = "***";

  public static final String MASK_COLUMN = "identifier";

  public static final String MASK_TIMESTAMP_COLUMN = "time_identifier";

  public static final String MASK_METADATA_COLUMN = "meta_identifier";

  public static String maskField(String fieldName) {
    if (fieldName.equals(INTERNAL_TIMESTAMP)) {
      return MASK_TIMESTAMP_COLUMN;
    }
    if (fieldName.startsWith("_")) {
      return MASK_METADATA_COLUMN;
    }
    return MASK_COLUMN;
  }

  // For field names, we typically don't escape dots as they're used for nested fields
  // But we escape other special characters
  public static final String LUCENE_SPECIAL_CHARS = "+-&|!(){}[]^\"~:/";

  /**
   * Escape field name for query_string syntax. Only spaces need to be escaped in field names. Other
   * special characters are handled automatically by the query parser in field position.
   *
   * @param fieldName the field name to escape
   * @return escaped field name
   */
  public static String escapeFieldName(String fieldName) {
    // Only escape spaces in field names
    return fieldName.replace(" ", "\\ ");
  }

  /**
   * Escape Lucene/query_string special characters. Special characters: + - && || ! ( ) { } [ ] ^ "
   * ~ : / Note: * and ? are NOT escaped to allow wildcard pattern matching
   *
   * @param text the text to escape
   * @return escaped text with wildcards preserved
   */
  public static String escapeLuceneSpecialCharacters(String text) {
    StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);

      // Check if this is a special character that needs escaping
      if (LUCENE_SPECIAL_CHARS.indexOf(c) >= 0) {
        // Special handling for && and ||
        if ((c == '&' || c == '|') && i + 1 < text.length() && text.charAt(i + 1) == c) {
          // Escape double && or ||
          escaped.append('\\').append(c).append('\\').append(c);
          i++; // Skip next character as we've handled it
        } else {
          // Escape single special character
          escaped.append('\\').append(c);
        }
      } else {
        escaped.append(c);
      }
    }

    return escaped.toString();
  }
}
