/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprWeekday;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.expression.function.FunctionProperties;

public class WeekDayFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    if (sqlTypeName == SqlTypeName.TIME) {
      return formatNow(restored.getQueryStartClock()).getDayOfWeek().getValue() - 1;
    } else {
      return exprWeekday(transferInputToExprValue(args[0], sqlTypeName)).integerValue();
    }
  }
}
