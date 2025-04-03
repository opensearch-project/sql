/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSeconds;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSecondsForIntType;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class ToSecondsFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    switch (sqlTypeName) {
      case DATE, TIME, TIMESTAMP, CHAR, VARCHAR: // need to transfer to timestamp firstly
        ExprValue dateTimeValue = transferInputToExprTimestampValue(args[0], sqlTypeName, restored);
        return exprToSeconds(dateTimeValue).longValue();
      default:
        return exprToSecondsForIntType(new ExprLongValue((Number) args[0])).longValue();
    }
  }
}
