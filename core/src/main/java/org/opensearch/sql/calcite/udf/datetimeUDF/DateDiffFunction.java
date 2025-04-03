/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;

/**
 * Calculates the difference of date parts of given values. If the first argument is time, today's
 * date is used.
 *
 * <p>(DATE/TIMESTAMP/TIME, DATE/TIMESTAMP/TIME) -> LONG
 */
public class DateDiffFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    SqlTypeName sqlTypeName1 = (SqlTypeName) args[1];
    SqlTypeName sqlTypeName2 = (SqlTypeName) args[3];
    ExprValue diffResult =
        DateTimeFunctions.exprDateDiff(
            restored,
            fromObjectValue(args[0], convertSqlTypeNameToExprType(sqlTypeName1)),
            fromObjectValue(args[2], convertSqlTypeNameToExprType(sqlTypeName2)));
    return diffResult.longValue();
  }
}
