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
import org.opensearch.sql.expression.function.FunctionProperties;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

// TODO: Fix MICROSECOND precision, it is not correct with Calcite timestamp
public class ExtractFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argPart = args[0];
    Object argTimestamp = args[1];
    SqlTypeName argType = (SqlTypeName) args[2];

    ExprValue candidate = fromObjectValue(argTimestamp, convertSqlTypeNameToExprType(argType));
    if (argType == SqlTypeName.TIME) {
      FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
      return DateTimeFunctions.exprExtractForTime(
                      restored, new ExprStringValue(argPart.toString()), candidate)
              .longValue();
    }
    return DateTimeFunctions.formatExtractFunction(
            new ExprStringValue(argPart.toString()), candidate)
        .longValue();
  }
}
