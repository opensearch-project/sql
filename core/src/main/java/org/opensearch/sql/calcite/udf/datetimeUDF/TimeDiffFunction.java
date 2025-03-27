/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimeDiff;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;

public class TimeDiffFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    SqlTypeName startType = (SqlTypeName) args[2];
    SqlTypeName endType = (SqlTypeName) args[3];
    Instant startTime = InstantUtils.convertToInstant(args[0], startType, false);
    Instant endTime = InstantUtils.convertToInstant(args[1], endType, false);
    ExprValue diffValue =
        exprTimeDiff(
            new ExprTimeValue(LocalDateTime.ofInstant(startTime, ZoneOffset.UTC).toLocalTime()),
            new ExprTimeValue(LocalDateTime.ofInstant(endTime, ZoneOffset.UTC).toLocalTime()));
    return formatTime(diffValue.timeValue());
    //return java.sql.Time.valueOf(diffValue.timeValue());
  }
}
