/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestamp;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAdd;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAddForTimeType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class TimestampAddFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    String addUnit = (String) args[0];
    int amount = (int) args[1];
    SqlTypeName sqlTypeName = (SqlTypeName) args[3];
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue timestampBase = transferInputToExprValue(args[2], sqlTypeName);
    if (sqlTypeName == SqlTypeName.TIME) {
      return exprTimestampAddForTimeType(
          restored.getQueryStartClock(),
          new ExprStringValue(addUnit),
          new ExprLongValue(amount),
          timestampBase);
    }
    ExprValue returnValue =
        exprTimestampAdd(new ExprStringValue(addUnit), new ExprLongValue(amount), timestampBase);
    return formatTimestamp(LocalDateTime.ofInstant(returnValue.timestampValue(), ZoneOffset.UTC));
  }
}
