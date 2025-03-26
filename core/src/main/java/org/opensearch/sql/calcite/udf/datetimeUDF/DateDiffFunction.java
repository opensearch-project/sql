/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprTimestampValue;
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
    SqlTypeName sqlTypeName1 = (SqlTypeName) args[1];
    Instant timestamp1 = InstantUtils.convertToInstant(args[0], sqlTypeName1);
    SqlTypeName sqlTypeName2 = (SqlTypeName) args[3];
    Instant timestamp2 = InstantUtils.convertToInstant(args[2], sqlTypeName2);
    LocalDateTime localDateTime1 = LocalDateTime.ofInstant(timestamp1, ZoneOffset.UTC);
    LocalDateTime localDateTime2 = LocalDateTime.ofInstant(timestamp2, ZoneOffset.UTC);
    ExprValue diffResult =
        DateTimeFunctions.exprDateDiff(
            new FunctionProperties(),
            new ExprTimestampValue(localDateTime1),
            new ExprTimestampValue(localDateTime2));
    return diffResult.longValue();
  }
}
