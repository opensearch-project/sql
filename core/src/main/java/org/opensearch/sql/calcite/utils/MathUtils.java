/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

public class MathUtils {
  public static final double EPSILON = 0.0000001;

  public static boolean isIntegral(Number n) {
    return n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long;
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
}
