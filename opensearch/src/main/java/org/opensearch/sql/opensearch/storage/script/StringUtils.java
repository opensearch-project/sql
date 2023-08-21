/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StringUtils {
  /**
   * Converts sql wildcard character % and _ to * and ?.
   *
   * @param text string to be converted
   * @return converted string
   */
  public static String convertSqlWildcardToLucene(String text) {
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
        default:
          convertedString.append(currentChar);
          escaped = false;
      }
    }
    return convertedString.toString();
  }
}
