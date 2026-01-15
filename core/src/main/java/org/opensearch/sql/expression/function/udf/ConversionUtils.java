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

  private ConversionUtils() {
    // Utility class - prevent instantiation
  }

  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Pattern LEADING_NUMBER_WITH_UNIT_PATTERN =
      Pattern.compile("^([+-]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][+-]?\\d+)?)(.*)$");
  private static final Pattern CONTAINS_LETTER_PATTERN = Pattern.compile(".*[a-zA-Z].*");
  private static final Pattern STARTS_WITH_SIGN_OR_DIGIT = Pattern.compile("^[+-]?[\\d.].*");

  /** Conversion strategy for different convert functions. */
  private enum ConversionStrategy {
    STANDARD, // num() - fixed numeric conversion
    COMPREHENSIVE, // auto() - extensible, will add new features later
    COMMA_ONLY, // rmcomma() - only comma removal
    UNIT_ONLY // rmunit() - only unit removal
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
    return STARTS_WITH_SIGN_OR_DIGIT.matcher(str).matches();
  }

  /** Unified conversion method that applies different strategies. */
  private static Object convert(Object value, ConversionStrategy strategy) {
    if ((strategy == ConversionStrategy.STANDARD || strategy == ConversionStrategy.COMPREHENSIVE)
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

    if (CONTAINS_LETTER_PATTERN.matcher(str).matches()) {
      return tryConvertWithUnitRemoval(str);
    }

    return tryConvertWithCommaRemoval(str);
  }

  private static Object convertComprehensive(String str) {
    // Future: Add new conversion strategies here before delegating
    // e.g., tryTimeConversion(str), etc
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
}
