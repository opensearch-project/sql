/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSeconds;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSecondsForIntType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;

public class ToSecondsFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    switch (sqlTypeName) {
      case DATE, TIME, TIMESTAMP, CHAR, VARCHAR:
        ExprValue dateTimeValue = fromObjectValue(args[0], convertSqlTypeNameToExprType(sqlTypeName));
        return exprToSeconds(dateTimeValue).longValue();
      default:
        return exprToSecondsForIntType(new ExprLongValue((Number) args[0])).longValue();
    }
  }
}
