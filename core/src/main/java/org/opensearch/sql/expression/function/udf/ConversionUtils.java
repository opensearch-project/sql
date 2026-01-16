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

  private ConversionUtils() {}

  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Pattern LEADING_NUMBER_WITH_UNIT_PATTERN =
      Pattern.compile("^([+-]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][+-]?\\d+)?)(.*)$");
  private static final Pattern CONTAINS_LETTER_PATTERN = Pattern.compile(".*[a-zA-Z].*");
  private static final Pattern STARTS_WITH_SIGN_OR_DIGIT = Pattern.compile("^[+-]?[\\d.].*");
  private static final Pattern MEMK_PATTERN = Pattern.compile("^([+-]?\\d+\\.?\\d*)([kmgKMG])?$");

  private static final double MB_TO_KB = 1024.0;
  private static final double GB_TO_KB = 1024.0 * 1024.0;

  private enum ConversionStrategy {
    STANDARD, // num()
    COMPREHENSIVE, // auto()
    COMMA_ONLY, // rmcomma()
    UNIT_ONLY, // rmunit()
    MEMK // memk()
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
      return Double.parseDouble(str);
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

  private static Double tryConvertWithCommaRemoval(String str) {
    String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
    return tryParseDouble(noCommas);
  }

  private static boolean isPotentiallyConvertible(String str) {
    return STARTS_WITH_SIGN_OR_DIGIT.matcher(str).matches();
  }

  /**
   * Check if string has a valid unit suffix (not a malformed number).
   */
  private static boolean hasValidUnitSuffix(String str, String leadingNumber) {
    if (leadingNumber == null || leadingNumber.length() >= str.length()) {
      return false;
    }
    String suffix = str.substring(leadingNumber.length()).trim();
    if (suffix.isEmpty()) {
      return false;
    }
    char firstChar = suffix.charAt(0);
    return !Character.isDigit(firstChar) && firstChar != '.';
  }

  /** Unified conversion method that applies different strategies. */
  private static Object convert(Object value, ConversionStrategy strategy) {
    if ((strategy == ConversionStrategy.STANDARD
            || strategy == ConversionStrategy.COMPREHENSIVE
            || strategy == ConversionStrategy.MEMK)
        && value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    switch (strategy) {
      case STANDARD:
        return convertStandard(str);
      case COMPREHENSIVE:
        return convertComprehensive(str);
      case COMMA_ONLY:
        return convertCommaOnly(str);
      case UNIT_ONLY:
        return convertUnitOnly(str);
      case MEMK:
        return convertMemk(str);
      default:
        return null;
    }
  }

  private static Object convertStandard(String str) {
    if (!isPotentiallyConvertible(str)) {
      return null;
    }

    Double result = tryParseDouble(str);
    if (result != null) {
      return result;
    }

    if (str.contains(",")) {
      result = tryConvertWithCommaRemoval(str);
      if (result != null) {
        return result;
      }
    }

    String leadingNumber = extractLeadingNumber(str);
    if (hasValidUnitSuffix(str, leadingNumber)) {
      return tryParseDouble(leadingNumber);
    }

    return null;
  }

  private static Object convertComprehensive(String str) {
    Object memkResult = convertMemk(str);
    if (memkResult != null) {
      return memkResult;
    }
    return convertStandard(str);
  }

  private static Object convertCommaOnly(String str) {
    if (CONTAINS_LETTER_PATTERN.matcher(str).matches()) {
      return null;
    }
    return tryConvertWithCommaRemoval(str);
  }

  private static Object convertUnitOnly(String str) {
    String numberStr = extractLeadingNumber(str);
    return numberStr != null ? tryParseDouble(numberStr) : null;
  }

  private static Object convertMemk(String str) {
    Matcher matcher = MEMK_PATTERN.matcher(str);
    if (!matcher.matches()) {
      return null;
    }

    Double number = tryParseDouble(matcher.group(1));
    if (number == null) {
      return null;
    }

    String unit = matcher.group(2);
    if (unit == null || unit.equalsIgnoreCase("k")) {
      return number;
    }

    double multiplier =
        switch (unit.toLowerCase()) {
          case "m" -> MB_TO_KB;
          case "g" -> GB_TO_KB;
          default -> 1.0;
        };

    return number * multiplier;
  }

  public static Object autoConvert(Object value) {
    return convert(value, ConversionStrategy.COMPREHENSIVE);
  }

  public static Object numConvert(Object value) {
    return convert(value, ConversionStrategy.STANDARD);
  }

  public static Object rmcommaConvert(Object value) {
    return convert(value, ConversionStrategy.COMMA_ONLY);
  }

  public static Object rmunitConvert(Object value) {
    return convert(value, ConversionStrategy.UNIT_ONLY);
  }

  public static Object memkConvert(Object value) {
    return convert(value, ConversionStrategy.MEMK);
  }
}
