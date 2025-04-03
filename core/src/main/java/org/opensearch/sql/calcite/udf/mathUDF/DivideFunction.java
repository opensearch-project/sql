/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

public class DivideFunction implements UserDefinedFunction {

  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    Number dividend = (Number) args[0];
    Number divisor = (Number) args[1];

    if (Math.abs(divisor.doubleValue()) < MathUtils.EPSILON) {
      return null;
    }

    double result = dividend.doubleValue() / divisor.doubleValue();
    if (MathUtils.isIntegral(dividend) && MathUtils.isIntegral(divisor)) {
      return MathUtils.coerceToWidestIntegralType(dividend, divisor, (long) result);
    }
    return MathUtils.coerceToWidestFloatingType(dividend, divisor, result);
  }
}
