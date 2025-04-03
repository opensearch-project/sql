/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSecToTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSecToTimeWithNanos;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

public class SecondToTimeFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Number candidate = (Number) args[0];
    ExprValue returnTimeValue;
    ExprValue transferredValue;
    if (candidate instanceof Long) {
      transferredValue = ExprValueUtils.longValue((Long) candidate);
      returnTimeValue = exprSecToTime(transferredValue);
    } else if (candidate instanceof Integer) {
      transferredValue = ExprValueUtils.integerValue((Integer) candidate);
      returnTimeValue = exprSecToTime(transferredValue);
    } else if (candidate instanceof Double) {
      transferredValue = ExprValueUtils.doubleValue((Double) candidate);
      returnTimeValue = exprSecToTimeWithNanos(transferredValue);
    } else {
      transferredValue = ExprValueUtils.floatValue((Float) candidate);
      returnTimeValue = exprSecToTimeWithNanos(transferredValue);
    }
    return new ExprTimeValue(returnTimeValue.timeValue()).valueForCalcite();
  }
}
