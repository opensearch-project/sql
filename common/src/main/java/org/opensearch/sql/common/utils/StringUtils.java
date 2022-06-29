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
   * Replace doubled quotes within the string with single version
   * @param text string
   * @return An unquoted string whose outer pair of (single/double/back-tick) quotes have been
   *     removed
   */
  public static String unquoteText(String text) {

    if (isQuoted(text, "\"")) {
      text = text.replace("\"\"", "\"");
      text = text.substring(1, text.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
      return text;
    }
    if (isQuoted(text, "'")) {
      text = text.replace("''", "'");
      text = text.substring(1, text.length() - 1).replace("\\'", "'").replace("\\\\", "\\");
      return text;
    }
    if (isQuoted(text, "`")) {
      text =  text.substring(1, text.length() - 1).replace("\\`", "`").replace("\\\\", "\\");
      return text;
    }
    return text;
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
