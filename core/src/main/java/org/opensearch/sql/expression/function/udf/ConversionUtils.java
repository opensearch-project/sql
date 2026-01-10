/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.regex.Pattern;

/** Utility class for conversion functions used by convert command UDFs. */
public class ConversionUtils {

  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Pattern LEADING_NUMBER_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)");

  /**
   * Auto convert field value to numeric type using best-fit heuristics. Tries conversions in order:
   * direct numeric, remove commas, extract leading numbers. Returns Long for integers, Double for
   * decimals.
   */
  public static Object autoConvert(Object value) {
    if (value == null) return null;

    // If already a number, return as-is (preserve Long for integers, Double for decimals)
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      return ((Number) value).longValue();
    }
    if (value instanceof Double || value instanceof Float) {
      return ((Number) value).doubleValue();
    }

    String str = value.toString().trim();
    if (str.isEmpty()) return null;

    // Step 1: Try direct number conversion first (num() functionality)
    try {
      if (str.contains(".")) {
        return Double.parseDouble(str);
      } else {
        return Long.parseLong(str);
      }
    } catch (NumberFormatException e) {
      // Step 2: Try removing commas then convert (rmcomma() + num())
      String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
      try {
        if (noCommas.contains(".")) {
          return Double.parseDouble(noCommas);
        } else {
          return Long.parseLong(noCommas);
        }
      } catch (NumberFormatException e2) {
        // Step 3: Try extracting leading numbers (rmunit() functionality)
        var matcher = LEADING_NUMBER_PATTERN.matcher(noCommas);
        if (matcher.find()) {
          String numberStr = matcher.group(1);
          try {
            if (numberStr.contains(".")) {
              return Double.parseDouble(numberStr);
            } else {
              return Long.parseLong(numberStr);
            }
          } catch (NumberFormatException e3) {
            return null;
          }
        }
        return null;
      }
    }
  }

  /** Convert field value to number. Returns Long for integers, Double for decimals. */
  public static Object numConvert(Object value) {
    if (value == null) return null;

    // If already a number, return as-is (preserve Long for integers, Double for decimals)
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      return ((Number) value).longValue();
    }
    if (value instanceof Double || value instanceof Float) {
      return ((Number) value).doubleValue();
    }

    String str = value.toString().trim();
    if (str.isEmpty()) return null;

    try {
      if (str.contains(".")) {
        return Double.parseDouble(str);
      } else {
        return Long.parseLong(str);
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Remove commas from field value. */
  public static Object rmcommaConvert(Object value) {
    if (value == null) return null;
    return COMMA_PATTERN.matcher(value.toString()).replaceAll("");
  }

  /** Extract leading numbers and remove trailing text. */
  public static Object rmunitConvert(Object value) {
    if (value == null) return null;
    String str = value.toString().trim();
    if (str.isEmpty()) return null;

    var matcher = LEADING_NUMBER_PATTERN.matcher(str);
    if (matcher.find()) {
      String numberStr = matcher.group(1);
      try {
        if (numberStr.contains(".")) {
          return Double.parseDouble(numberStr);
        } else {
          return Long.parseLong(numberStr);
        }
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  // Overloaded methods for specific types
  public static Object autoConvert(String value) {
    return autoConvert((Object) value);
  }

  public static Object autoConvert(long value) {
    return autoConvert((Object) value);
  }

  public static Object autoConvert(Long value) {
    return autoConvert((Object) value);
  }

  public static Object autoConvert(double value) {
    return autoConvert((Object) value);
  }

  public static Object autoConvert(Double value) {
    return autoConvert((Object) value);
  }

  public static Object numConvert(String value) {
    return numConvert((Object) value);
  }

  public static Object numConvert(long value) {
    return numConvert((Object) value);
  }

  public static Object numConvert(Long value) {
    return numConvert((Object) value);
  }

  public static Object numConvert(double value) {
    return numConvert((Object) value);
  }

  public static Object numConvert(Double value) {
    return numConvert((Object) value);
  }

  public static Object rmcommaConvert(String value) {
    return rmcommaConvert((Object) value);
  }

  public static Object rmunitConvert(String value) {
    return rmunitConvert((Object) value);
  }
}
