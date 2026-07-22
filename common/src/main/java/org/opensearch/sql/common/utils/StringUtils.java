/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import com.google.common.base.Strings;
import java.util.Collection;
import java.util.IllegalFormatException;
import java.util.Locale;
import java.util.Optional;

public class StringUtils {
  /**
   * Unquote Identifier which has " or ' as mark. Strings quoted by ' or " with two of these quotes
   * appearing next to each other in the quote acts as an escape<br>
   * Example: 'Test''s' will result in 'Test's', similar with those single quotes being replaced
   * with double quote. Supports escaping quotes (single/double) and escape characters using the `\`
   * characters.
   *
   * @param text string
   * @return An unquoted string whose outer pair of (single/double) quotes have been removed
   */
  public static String unquoteText(String text) {
    if (text.length() < 2) {
      return text;
    }

    char enclosingQuote = 0;
    char firstChar = text.charAt(0);
    char lastChar = text.charAt(text.length() - 1);

    if (firstChar != lastChar) {
      return text;
    }

    if (firstChar == '`') {
      return text.substring(1, text.length() - 1);
    }

    if (firstChar == lastChar && (firstChar == '\'' || firstChar == '"')) {
      enclosingQuote = firstChar;
    } else {
      return text;
    }

    char currentChar;
    char nextChar;

    StringBuilder textSB = new StringBuilder();

    // Ignores first and last character as they are the quotes that should be removed
    for (int chIndex = 1; chIndex < text.length() - 1; chIndex++) {
      currentChar = text.charAt(chIndex);
      nextChar = text.charAt(chIndex + 1);

      if ((currentChar == '\\' && (nextChar == '"' || nextChar == '\\' || nextChar == '\''))
          || (currentChar == nextChar && currentChar == enclosingQuote)) {
        chIndex++;
        currentChar = nextChar;
      }
      textSB.append(currentChar);
    }
    return textSB.toString();
  }

  /**
   * Unquote Identifier which has ` as mark.
   *
   * @param identifier identifier that possibly enclosed by double quotes or back ticks
   * @return An unquoted string whose outer pair of (double/back-tick) quotes have been removed
   */
  public static String unquoteIdentifier(String identifier) {
    if (isQuoted(identifier, "`")) {
      return identifier.substring(1, identifier.length() - 1);
    } else {
      return identifier;
    }
  }

  /**
   * Returns a formatted string using the specified format string and arguments, as well as the
   * {@link Locale#ROOT} locale.
   *
   * @param format format string
   * @param args arguments referenced by the format specifiers in the format string
   * @return A formatted string
   * @throws IllegalFormatException If a format string contains an illegal syntax, a format
   *     specifier that is incompatible with the given arguments, insufficient arguments given the
   *     format string, or other illegal conditions.
   * @see java.lang.String#format(Locale, String, Object...)
   */
  public static String format(final String format, Object... args) {
    return String.format(Locale.ROOT, format, args);
  }

  private static boolean isQuoted(String text, String mark) {
    return !Strings.isNullOrEmpty(text) && text.startsWith(mark) && text.endsWith(mark);
  }

  /**
   * Calculates the Levenshtein distance between two strings.
   *
   * @param s1 first string
   * @param s2 second string
   * @return the Levenshtein distance between s1 and s2
   */
  public static int levenshteinDistance(String s1, String s2) {
    if (s1 == null || s2 == null) {
      return Integer.MAX_VALUE;
    }
    if (s1.equals(s2)) {
      return 0;
    }

    int len1 = s1.length();
    int len2 = s2.length();

    if (len1 == 0) {
      return len2;
    }
    if (len2 == 0) {
      return len1;
    }

    int[] prev = new int[len2 + 1];
    int[] curr = new int[len2 + 1];

    for (int j = 0; j <= len2; j++) {
      prev[j] = j;
    }

    for (int i = 1; i <= len1; i++) {
      curr[0] = i;
      for (int j = 1; j <= len2; j++) {
        int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
        curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
      }
      int[] temp = prev;
      prev = curr;
      curr = temp;
    }

    return prev[len2];
  }

  /**
   * Finds the closest match to a target string from a collection of candidates using Levenshtein
   * distance. Returns empty if no candidates are provided or if the best match distance is too
   * large.
   *
   * @param target the string to match against
   * @param candidates the collection of candidate strings
   * @return the closest match, or empty if no good match is found
   */
  public static Optional<String> findClosestMatch(String target, Collection<String> candidates) {
    if (target == null || candidates == null || candidates.isEmpty()) {
      return Optional.empty();
    }

    String bestMatch = null;
    int bestDistance = Integer.MAX_VALUE;

    for (String candidate : candidates) {
      int distance = levenshteinDistance(target.toLowerCase(), candidate.toLowerCase());
      if (distance < bestDistance) {
        bestDistance = distance;
        bestMatch = candidate;
      }
    }

    // Only return a suggestion if the distance is reasonable
    if (bestMatch != null && bestDistance <= Math.max(4, target.length() / 2)) {
      return Optional.of(bestMatch);
    }

    return Optional.empty();
  }
}
