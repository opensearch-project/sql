/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

/** Utility class for query_string syntax operations. */
public class QueryStringUtils {

  /**
   * Escape field name for query_string syntax. Field names typically shouldn't have spaces, but may
   * have dots or other characters.
   *
   * @param fieldName the field name to escape
   * @return escaped field name
   */
  public static String escapeFieldName(String fieldName) {
    // For field names, we typically don't escape dots as they're used for nested fields
    // But we escape other special characters
    String specialChars = "+-&|!(){}[]^\"~*?:\\/";

    StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < fieldName.length(); i++) {
      char c = fieldName.charAt(i);

      // Don't escape dots in field names as they indicate nested fields
      if (c != '.' && specialChars.indexOf(c) >= 0) {
        escaped.append('\\').append(c);
      } else {
        escaped.append(c);
      }
    }

    return escaped.toString();
  }

  /**
   * Escape Lucene/query_string special characters. Special characters: + - && || ! ( ) { } [ ] ^ "
   * ~ * ? : \ /
   *
   * @param text the text to escape
   * @return escaped text
   */
  public static String escapeLuceneSpecialCharacters(String text) {
    // List of special characters that need escaping
    String specialChars = "+-&|!(){}[]^\"~*?:\\/";

    StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);

      // Check if this is a special character that needs escaping
      if (specialChars.indexOf(c) >= 0) {
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
