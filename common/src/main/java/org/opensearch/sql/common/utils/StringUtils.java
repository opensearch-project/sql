/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.common.utils;

import com.google.common.base.Strings;
import java.util.IllegalFormatException;
import java.util.Locale;

public class StringUtils {
  /**
   * Unquote any string with mark specified.
   * @param text string
   * @param mark quotation mark
   * @return An unquoted string whose outer pair of (single/double/back-tick) quotes have been
   *     removed
   */
  public static String unquote(String text, String mark) {
    if (isQuoted(text, mark)) {
      return text.substring(mark.length(), text.length() - mark.length());
    }
    return text;
  }

  /**
   * Unquote Identifier which has " or ' or ` as mark.
   * Strings quoted by ' or " with two of these quotes appearing next to each other in the quote
   * acts as an escape
   * Example: 'Test''s' will result in 'Test's', similar with those single quotes being replaced
   * with double.
   * @param text string
   * @return An unquoted string whose outer pair of (single/double/back-tick) quotes have been
   *     removed
   */
  public static String unquoteText(String text) {

    if (text.length() < 2) {
      return text;
    }

    char enclosingQuote;
    char firstChar = text.charAt(0);
    char lastChar = text.charAt(text.length() - 1);

    if (firstChar == lastChar
            && (firstChar == '\''
            || firstChar == '"'
            || firstChar == '`')) {
      enclosingQuote = firstChar;
    } else {
      return text;
    }

    if (enclosingQuote == '`') {
      return text.substring(1, text.length() - 1);
    }

    char currentChar;
    char nextChar;

    StringBuilder textSB = new StringBuilder();

    // Ignores first and last character as they are the quotes that should be removed
    for (int chIndex = 1; chIndex < text.length() - 1; chIndex++) {
      currentChar = text.charAt(chIndex);
      nextChar = text.charAt(chIndex + 1);
      if (currentChar == enclosingQuote
              && nextChar == currentChar) {
        chIndex++;
      }
      textSB.append(currentChar);
    }

    return textSB.toString();
  }

  /**
   * Unquote Identifier which has ` as mark.
   * @param identifier identifier that possibly enclosed by double quotes or back ticks
   * @return An unquoted string whose outer pair of (double/back-tick) quotes have been
   *     removed
   */
  public static String unquoteIdentifier(String identifier) {
    if (isQuoted(identifier, "`")) {
      return identifier.substring(1, identifier.length() - 1);
    } else {
      return identifier;
    }
  }

  /**
   * Returns a formatted string using the specified format string and
   * arguments, as well as the {@link Locale#ROOT} locale.
   *
   * @param format format string
   * @param args   arguments referenced by the format specifiers in the format string
   * @return A formatted string
   * @throws IllegalFormatException If a format string contains an illegal syntax, a format
   *                                specifier that is incompatible with the given arguments,
   *                                insufficient arguments given the format string, or other
   *                                illegal conditions.
   * @see java.lang.String#format(Locale, String, Object...)
   */
  public static String format(final String format, Object... args) {
    return String.format(Locale.ROOT, format, args);
  }

  private static boolean isQuoted(String text, String mark) {
    return !Strings.isNullOrEmpty(text) && text.startsWith(mark) && text.endsWith(mark);
  }
}