/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestamp;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

/**
 * DATETIME(timestamp)/ DATETIME(date, to_timezone) Converts the datetime to a new timezone. If not
 * specified, the timestamp is regarded to be in system time zone.
 *
 * <p>(TIMESTAMP, STRING) -> TIMESTAMP <br>
 * (TIMESTAMP) -> TIMESTAMP
 *
 * <p>Converting timestamp with timezone to the second argument timezone.
 */
public class DatetimeFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argTimestamp = args[0];
    ExprValue argTimestampExpr = new ExprStringValue(argTimestamp.toString());
    ExprValue datetimeExpr;
    if (args.length == 1) {
      datetimeExpr = DateTimeFunctions.exprDateTimeNoTimezone(argTimestampExpr);
    } else {
      Object argTimezone = args[1];
      datetimeExpr =
          DateTimeFunctions.exprDateTime(
              argTimestampExpr, new ExprStringValue(argTimezone.toString()));
    }
    if (datetimeExpr.isNull()) {
      return null;
    }
    return formatTimestamp(LocalDateTime.ofInstant(datetimeExpr.timestampValue(), ZoneOffset.UTC));
  }
}
