/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/**
 * Calculate the remainder of x divided by y<br>
 * The supported signature of mod function is<br>
 * (x: INTEGER/LONG/FLOAT/DOUBLE, y: INTEGER/LONG/FLOAT/DOUBLE)<br>
 * -> wider type between types of x and y
 */
public class ModFunction implements UserDefinedFunction {

  @Override
  public Object eval(Object... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          String.format("MOD function requires exactly two arguments, but got %d", args.length));
    }

    Object arg0 = args[0];
    Object arg1 = args[1];
    if (!(arg0 instanceof Number num0) || !(arg1 instanceof Number num1)) {
      throw new IllegalArgumentException(
          String.format(
              "MOD function requires two numeric arguments, but got %s and %s",
              arg0.getClass().getSimpleName(), arg1.getClass().getSimpleName()));
    }

    // TODO: This precision check is arbitrary.
    if (Math.abs(num1.doubleValue()) < 0.0000001) {
      return null;
    }

    if (isIntegral(num0) && isIntegral(num1)) {
      long l0 = num0.longValue();
      long l1 = num1.longValue();
      // It returns negative values when l0 is negative
      long result = l0 % l1;
      // Return the wider type between l0 and l1
      if (num0 instanceof Integer && num1 instanceof Integer) {
        return (int) result;
      }
      return result;
    }

    double d0 = num0.doubleValue();
    double d1 = num1.doubleValue();
    return d0 % d1;
  }

  private boolean isIntegral(Number n) {
    return n instanceof Integer || n instanceof Long;
  }
}
