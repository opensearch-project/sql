/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampDiff;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampDiffForTimeType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class TimestampDiffFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    String addUnit = (String) args[0];
    SqlTypeName sqlTypeName1 = (SqlTypeName) args[2];
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue timestamp1 = transferInputToExprValue(args[1], sqlTypeName1);
    //Instant timestamp1;
    //timestamp1 = InstantUtils.convertToInstant(args[1], sqlTypeName1, false);
    SqlTypeName sqlTypeName2 = (SqlTypeName) args[4];
    ExprValue timestamp2 = transferInputToExprValue(args[3], sqlTypeName2);
    //timestamp2 = InstantUtils.convertToInstant(args[3], sqlTypeName2, false);
    //LocalDateTime localDateTime1 = LocalDateTime.ofInstant(timestamp1, ZoneOffset.UTC);
    //LocalDateTime localDateTime2 = LocalDateTime.ofInstant(timestamp2, ZoneOffset.UTC);
    if (sqlTypeName1 == SqlTypeName.TIME) {
      return exprTimestampDiffForTimeType(restored, new ExprStringValue(addUnit), timestamp1, timestamp2);
    }
    ExprValue diffResult =
        exprTimestampDiff(
            new ExprStringValue(addUnit),
            timestamp1, timestamp2);
    return diffResult.longValue();
  }
}
