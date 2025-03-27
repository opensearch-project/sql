/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class TimeFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    Object argTime = args[0];
    SqlTypeName argType = (SqlTypeName) args[1];

    ExprValue result;
    if (argType.equals(SqlTypeName.VARCHAR)) {
      result = DateTimeFunctions.exprTime(new ExprStringValue(argTime.toString()));
    } else {
      Instant instant = InstantUtils.convertToInstant(argTime, argType, false);
      result = DateTimeFunctions.exprTime(new ExprTimestampValue(instant));
    }

    return java.sql.Time.valueOf(result.timeValue());
  }
}
