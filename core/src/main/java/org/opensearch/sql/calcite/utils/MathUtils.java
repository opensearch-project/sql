/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.math.BigDecimal;

public class MathUtils {
  public static boolean isIntegral(Number n) {
    return n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long;
  }

  public static boolean isDecimal(Number n) {
    return n instanceof BigDecimal;
  }

  /**
   * Converts a long value to the least restrictive integral type based on the types of two input
   * numbers.
   *
   * <p>This is useful when performing operations like division or modulo and you want to preserve
   * the most appropriate type (e.g., int vs long).
   *
   * @param a one operand involved in the operation
   * @param b another operand involved in the operation
   * @param value the result to convert to the least restrictive integral type
   * @return the value converted to Byte, Short, Integer, or Long
   */
  public static Number coerceToWidestIntegralType(Number a, Number b, long value) {
    if (a instanceof Long || b instanceof Long) {
      return value;
    } else if (a instanceof Integer || b instanceof Integer) {
      return (int) value;
    } else if (a instanceof Short || b instanceof Short) {
      return (short) value;
    } else {
      return (byte) value;
    }
  }

  /**
   * Converts a double value to the least restrictive floating type based on the types of two input
   *
   * @param a one operand involved in the operation
   * @param b another operand involved in the operation
   * @param value the result to convert to the least restrictive floating type
   * @return the value converted to Float or Double
   */
  public static Number coerceToWidestFloatingType(Number a, Number b, double value) {
    if (a instanceof Double || b instanceof Double) {
      return value;
    } else {
      return (float) value;
    }
  }

  public static Number integralCosh(Number x) {
    double x0 = x.doubleValue();
    return Math.cosh(x0);
  }

  public static Number floatingCosh(Number x) {
    BigDecimal x0 = new BigDecimal(x.toString());
    return Math.cosh(x0.doubleValue());
  }

  public static Number integralSinh(Number x) {
    double x0 = x.doubleValue();
    return Math.sinh(x0);
  }

  public static Number floatingSinh(Number x) {
    BigDecimal x0 = new BigDecimal(x.toString());
    return Math.sinh(x0.doubleValue());
  }

  public static Number integralExpm1(Number x) {
    double x0 = x.doubleValue();
    return Math.expm1(x0);
  }

  public static Number floatingExpm1(Number x) {
    BigDecimal x0 = new BigDecimal(x.toString());
    return Math.expm1(x0.doubleValue());
  }

  public static Number integralRint(Number x) {
    double x0 = x.doubleValue();
    return Math.rint(x0);
  }

  public static Number floatingRint(Number x) {
    BigDecimal x0 = new BigDecimal(x.toString());
    return Math.rint(x0.doubleValue());
  }

  public static Number integralSignum(Number x) {
    double x0 = x.doubleValue();
    return (int) Math.signum(x0);
  }

  public static Number floatingSignum(Number x) {
    BigDecimal x0 = new BigDecimal(x.toString());
    return (int) Math.signum(x0.doubleValue());
  }
}
