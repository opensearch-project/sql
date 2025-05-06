/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class ConvertTZFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    Object argTimestamp = args[0];
    Object fromTz = args[1];
    Object toTz = args[2];
    ExprValue datetimeExpr =
        DateTimeFunctions.exprConvertTZ(
            new ExprStringValue(argTimestamp.toString()),
            new ExprStringValue(fromTz.toString()),
            new ExprStringValue(toTz.toString()));

    return datetimeExpr.valueForCalcite();
  }
}
