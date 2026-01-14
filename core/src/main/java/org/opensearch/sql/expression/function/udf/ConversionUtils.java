/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConversionUtils {

  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Pattern LEADING_NUMBER_WITH_UNIT_PATTERN =
      Pattern.compile("^([+-]?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)(.*)$");
  private static final Pattern CONTAINS_LETTER_PATTERN = Pattern.compile(".*[a-zA-Z].*");
  private static final Pattern STARTS_WITH_SIGN_OR_DIGIT = Pattern.compile("^[+-]?\\d.*");

  private static boolean isNumericType(Object value) {
    return value instanceof Number;
  }

  private static String preprocessValue(Object value) {
    if (value == null) {
      return null;
    }
    String str = value instanceof String ? ((String) value).trim() : value.toString().trim();
    return str.isEmpty() ? null : str;
  }

  private static Double tryParseDouble(String str) {
    try {
      Double result = Double.parseDouble(str);
      if (result.isInfinite()) {
        return null;
      }
      return result;
    } catch (NumberFormatException e) {
      log.debug("Failed to parse '{}' as number", str, e);
      return null;
    }
  }

  private static String extractLeadingNumber(String str) {
    Matcher matcher = LEADING_NUMBER_WITH_UNIT_PATTERN.matcher(str);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }

  private static Double tryConvertWithUnitRemoval(String str) {
    String leadingNumber = extractLeadingNumber(str);
    if (leadingNumber != null) {
      return tryParseDouble(leadingNumber);
    }
    return null;
  }

  private static Double tryConvertWithCommaRemoval(String str) {
    String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
    return tryParseDouble(noCommas);
  }

  private static boolean isPotentiallyConvertible(String str) {
    return STARTS_WITH_SIGN_OR_DIGIT.matcher(str).matches() || isNaN(str);
  }

  private static boolean isNaN(String str) {
    return "NaN".equals(str);
  }

  public static Object autoConvert(Object value) {
    if (isNumericType(value)) {
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    if (isNaN(str)) {
      return Double.NaN;
    }

    if (!isPotentiallyConvertible(str)) {
      return null;
    }

    Double result = tryParseDouble(str);
    if (result != null) {
      return result;
    }

    if (CONTAINS_LETTER_PATTERN.matcher(str).matches()) {
      return tryConvertWithUnitRemoval(str);
    }

    return tryConvertWithCommaRemoval(str);
  }

  public static Object numConvert(Object value) {
    if (isNumericType(value)) {
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    if (isNaN(str)) {
      return Double.NaN;
    }

    Double result = tryParseDouble(str);
    if (result != null) {
      return result;
    }

    if (CONTAINS_LETTER_PATTERN.matcher(str).matches()) {
      return tryConvertWithUnitRemoval(str);
    }

    String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
    return tryParseDouble(noCommas);
  }

  public static Object rmcommaConvert(Object value) {
    String str = preprocessValue(value);
    if (str == null || CONTAINS_LETTER_PATTERN.matcher(str).matches()) {
      return null;
    }
    String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
    return tryParseDouble(noCommas);
  }

  public static Object rmunitConvert(Object value) {
    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    String numberStr = extractLeadingNumber(str);
    return numberStr != null ? tryParseDouble(numberStr) : null;
  }
}
