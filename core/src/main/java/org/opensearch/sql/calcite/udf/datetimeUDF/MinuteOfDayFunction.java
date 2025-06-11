/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

/** minute(time) returns the amount of minutes in the day, in the range of 0 to 1439. */
public class MinuteOfDayFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    ExprValue candidate =
        fromObjectValue(args[0], convertSqlTypeNameToExprType((SqlTypeName) args[1]));
    return DateTimeFunctions.exprMinuteOfDay(candidate).integerValue();
  }
}
