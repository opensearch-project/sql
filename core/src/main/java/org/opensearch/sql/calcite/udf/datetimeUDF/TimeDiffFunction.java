/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimeDiff;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;

public class TimeDiffFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    SqlTypeName startType = (SqlTypeName) args[2];
    SqlTypeName endType = (SqlTypeName) args[3];
    ExprValue diffValue =
        exprTimeDiff(
            transferInputToExprValue(args[0], startType),
            transferInputToExprValue(args[1], endType));
    return new ExprTimeValue(diffValue.timeValue()).valueForCalcite();
  }
}
