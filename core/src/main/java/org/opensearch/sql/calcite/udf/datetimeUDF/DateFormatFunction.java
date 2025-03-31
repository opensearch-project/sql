/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedDate;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedDateOfToday;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class DateFormatFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argDatetime = args[0];
    Object argDatetimeType = args[1];
    Object argFormat = args[2];
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue candidateValue = transferInputToExprValue(argDatetime, (SqlTypeName) argDatetimeType);
    if (argDatetimeType == SqlTypeName.TIME) {
      return getFormattedDateOfToday(
          new ExprStringValue(argFormat.toString()), candidateValue, restored.getQueryStartClock());
    }
    return getFormattedDate(candidateValue, new ExprStringValue(argFormat.toString()));
  }
}
