/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;

@UtilityClass
public class OperatorUtils {
  /**
   * Wildcard pattern matcher util. Percent (%) character for wildcard, Underscore (_) character for
   * a single character match.
   *
   * @param pattern string pattern to match.
   * @return if text matches pattern returns true; else return false.
   */
  public static ExprBooleanValue matches(ExprValue text, ExprValue pattern) {
    return ExprBooleanValue.of(
        Pattern.compile(patternToRegex(pattern.stringValue()), Pattern.CASE_INSENSITIVE)
            .matcher(text.stringValue())
            .matches());
  }

  /**
   * Checks if text matches regular expression pattern.
   *
   * @param pattern string pattern to match.
   * @return if text matches pattern returns true; else return false.
   */
  public static ExprIntegerValue matchesRegexp(ExprValue text, ExprValue pattern) {
    return new ExprIntegerValue(
        Pattern.compile(pattern.stringValue()).matcher(text.stringValue()).matches() ? 1 : 0);
  }

  private static final char DEFAULT_ESCAPE = '\\';

  private static String patternToRegex(String patternString) {
    StringBuilder regex = new StringBuilder(patternString.length() * 2);
    regex.append('^');
    boolean escaped = false;
    for (char currentChar : patternString.toCharArray()) {
      if (!escaped && currentChar == DEFAULT_ESCAPE) {
        escaped = true;
      } else {
        switch (currentChar) {
          case '%':
            if (escaped) {
              regex.append("%");
            } else {
              regex.append(".*");
            }
            escaped = false;
            break;
          case '_':
            if (escaped) {
              regex.append("_");
            } else {
              regex.append('.');
            }
            escaped = false;
            break;
          default:
            switch (currentChar) {
              case '\\':
              case '^':
              case '$':
              case '.':
              case '*':
              case '[':
              case ']':
              case '(':
              case ')':
              case '|':
              case '+':
                regex.append('\\');
                break;
              default:
            }

            regex.append(currentChar);
            escaped = false;
        }
      }
    }
    regex.append('$');
    return regex.toString();
  }
}
