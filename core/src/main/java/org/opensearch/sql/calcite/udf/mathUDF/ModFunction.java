/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import java.math.BigDecimal;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.MathUtils;

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
    if (!(arg0 instanceof Number) || !(arg1 instanceof Number)) {
      throw new IllegalArgumentException(
          String.format(
              "MOD function requires two numeric arguments, but got %s and %s",
              arg0.getClass().getSimpleName(), arg1.getClass().getSimpleName()));
    }
    Number num0 = (Number) arg0;
    Number num1 = (Number) arg1;

    if (num1.doubleValue() == 0) {
      return null;
    }

    if (MathUtils.isIntegral(num0) && MathUtils.isIntegral(num1)) {
      long l0 = num0.longValue();
      long l1 = num1.longValue();
      // It returns negative values when l0 is negative
      long result = l0 % l1;
      // Return the wider type between l0 and l1
      return MathUtils.coerceToWidestIntegralType(num0, num1, result);
    }

    BigDecimal b0 = new BigDecimal(num0.toString());
    BigDecimal b1 = new BigDecimal(num1.toString());
    BigDecimal result = b0.remainder(b1);
    return MathUtils.coerceToWidestFloatingType(num0, num1, result.doubleValue());
  }
}
