/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.runtime.SqlFunctions;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
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
    // Manually convert to calcite internal representation of Timestamp to circumvent
    // errors relating to null returns
    return SqlFunctions.toLong(
        Timestamp.valueOf(LocalDateTime.ofInstant(datetimeExpr.timestampValue(), ZoneOffset.UTC)));
  }
}
