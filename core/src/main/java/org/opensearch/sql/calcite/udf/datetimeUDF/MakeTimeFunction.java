/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Time;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class MakeTimeFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    ExprValue timeExpr =
        DateTimeFunctions.exprMakeTime(
            new ExprDoubleValue(((Number) args[0]).doubleValue()),
            new ExprDoubleValue(((Number) args[1]).doubleValue()),
            new ExprDoubleValue(((Number) args[2]).doubleValue()));
    return Time.valueOf(timeExpr.timeValue());
  }
}
