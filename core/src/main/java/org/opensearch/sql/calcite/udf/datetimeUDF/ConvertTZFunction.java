package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class ConvertTZFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object argTimestamp = args[0];
    Object fromTz = args[1];
    Object toTz = args[2];

    Instant datetimeInstant = InstantUtils.fromEpochMills(((Number) argTimestamp).longValue());
    ExprValue datetimeExpr =
        DateTimeFunctions.exprConvertTZ(
                new ExprTimestampValue(datetimeInstant),
                new ExprStringValue(fromTz.toString()),
                new ExprStringValue(toTz.toString()));

    if (datetimeExpr.isNull()){
        return null;
    }

    return Timestamp.valueOf(LocalDateTime.ofInstant(datetimeExpr.timestampValue(), ZoneOffset.UTC));
  }
}
