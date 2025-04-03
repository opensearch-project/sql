/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.yearweekToday;

import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class YearWeekFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    int mode;
    SqlTypeName sqlTypeName;
    ExprValue exprValue;
    if (args.length == 3) {
      sqlTypeName = (SqlTypeName) args[1];
      mode = 0;
    } else {
      sqlTypeName = (SqlTypeName) args[2];
      mode = (int) args[1];
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    if (sqlTypeName == SqlTypeName.TIME) {
      return yearweekToday(new ExprIntegerValue(mode), restored.getQueryStartClock())
          .integerValue();
    }
    exprValue = transferInputToExprValue(args[0], sqlTypeName);
    ExprValue yearWeekValue = exprYearweek(exprValue, new ExprIntegerValue(mode));
    return yearWeekValue.integerValue();
  }
}
