/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import static java.lang.Math.sqrt;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class SqrtFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length < 1) {
      throw new IllegalArgumentException("At least one argument is required");
    }

    // Get the input value
    Object input = args[0];

    // Handle numbers dynamically
    if (input instanceof Number) {
      double num = ((Number) input).doubleValue();

      if (num < 0) {
        throw new ArithmeticException("Cannot compute square root of a negative number");
      }

      return sqrt(num); // Computes sqrt using Math.sqrt()
    } else {
      throw new IllegalArgumentException("Invalid argument type: Expected a numeric value");
    }
  }
}
