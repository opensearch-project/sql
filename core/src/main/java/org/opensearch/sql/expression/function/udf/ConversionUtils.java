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

  public static final Pattern COMMA_PATTERN = Pattern.compile(",");
  public static final Pattern LEADING_NUMBER_WITH_UNIT_PATTERN =
      Pattern.compile("^([+-]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][+-]?\\d+)?)(.*)$");
  public static final Pattern CONTAINS_LETTER_PATTERN = Pattern.compile(".*[a-zA-Z].*");
  public static final Pattern STARTS_WITH_SIGN_OR_DIGIT = Pattern.compile("^[+-]?[\\d.].*");
  public static final Pattern MEMK_PATTERN = Pattern.compile("^([+-]?\\d+\\.?\\d*)([kmgKMG])?$");

  public static final double MB_TO_KB = 1024.0;
  public static final double GB_TO_KB = 1024.0 * 1024.0;

  public static String preprocessValue(Object value) {
    if (value == null) {
      return null;
    }
    String str = value instanceof String ? ((String) value).trim() : value.toString().trim();
    return str.isEmpty() ? null : str;
  }

  public static Double tryParseDouble(String str) {
    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException e) {
      log.debug("Failed to parse '{}' as number", str, e);
      return null;
    }
  }

  public static String extractLeadingNumber(String str) {
    Matcher matcher = LEADING_NUMBER_WITH_UNIT_PATTERN.matcher(str);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }

  public static Double tryConvertWithCommaRemoval(String str) {
    String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
    return tryParseDouble(noCommas);
  }

  public static boolean isPotentiallyConvertible(String str) {
    return STARTS_WITH_SIGN_OR_DIGIT.matcher(str).matches();
  }

  public static boolean hasValidUnitSuffix(String str, String leadingNumber) {
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

  public static Double tryConvertMemoryUnit(String str) {
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
}
