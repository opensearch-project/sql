/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.dayOfYearToday;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprDayOfYear;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class DayOfYearFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue candidate = transferInputToExprValue(args[0], (SqlTypeName) args[1]);
    if ((SqlTypeName) args[1] == SqlTypeName.TIME) {
      return extractForTime(candidate, restored).valueForCalcite();
    }
    return extract(candidate).valueForCalcite();
  }

  public ExprValue extractForTime(ExprValue candidate, FunctionProperties functionProperties) {
    return dayOfYearToday(functionProperties.getQueryStartClock());
  }

  public ExprValue extract(ExprValue candidate) {
    return exprDayOfYear(candidate);
  }
}
