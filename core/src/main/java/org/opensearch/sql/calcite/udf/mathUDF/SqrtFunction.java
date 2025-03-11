/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import static java.lang.Math.sqrt;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/**
 * Calculate the square root of a non-negative number x<br>
 * The supported signature is<br>
 * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE It returns null if a negative parameter is provided
 */
public class SqrtFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length < 1) {
      throw new IllegalArgumentException("At least one argument is required");
    }

    Object input = args[0];

    if (input instanceof Number) {
      double num = ((Number) input).doubleValue();

      if (num < 0) {
        return null;
      }

      return (Double) sqrt((Double) num);
    } else {
      throw new IllegalArgumentException("Invalid argument type: Expected a numeric value");
    }
  }
}
