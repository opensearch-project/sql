/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StringUtils {
  /**
   * Converts sql wildcard character % and _ to * and ?. Also, DSL specific wildcard (* and ?) is
   * still supported. This function is used for legacy SQL WILDCARDQUERY and WILDCARD_QUERY
   * functions.
   *
   * @param text string to be converted
   * @return converted string
   */
  @Deprecated
  public static String convertSqlWildcardToLucene(String text) {
    return convert(text, false);
  }

  /**
   * Transforms a SQL like pattern into a Lucene/OpenSearch wildcard pattern.
   *
   * <p>It replaces '%' with '*' and '_' with '?' and escapes any literal '*' or '?' so they are
   * treated as ordinary characters.
   *
   * @param text string to be converted
   * @return converted string
   */
  public static String convertSqlWildcardToLuceneSafe(String text) {
    return convert(text, true);
  }

  private static String convert(String text, boolean escapeStarQuestion) {
    final char DEFAULT_ESCAPE = '\\';
    StringBuilder convertedString = new StringBuilder(text.length());
    boolean escaped = false;

    for (char currentChar : text.toCharArray()) {
      switch (currentChar) {
        case DEFAULT_ESCAPE:
          escaped = true;
          convertedString.append(currentChar);
          break;
        case '%':
          if (escaped) {
            convertedString.deleteCharAt(convertedString.length() - 1);
            convertedString.append("%");
          } else {
            convertedString.append("*");
          }
          escaped = false;
          break;
        case '_':
          if (escaped) {
            convertedString.deleteCharAt(convertedString.length() - 1);
            convertedString.append("_");
          } else {
            convertedString.append('?');
          }
          escaped = false;
          break;
        case '*':
        case '?':
          if (escapeStarQuestion && !escaped) {
            convertedString.append(DEFAULT_ESCAPE);
          }
          convertedString.append(currentChar);
          escaped = false;
          break;
        default:
          convertedString.append(currentChar);
          escaped = false;
      }
    }
    return convertedString.toString();
  }
}
