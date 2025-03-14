package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

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
            datetimeExpr = DateTimeFunctions.exprDateTime(argTimestampExpr, new ExprStringValue(argTimezone.toString()));
        }
        if (datetimeExpr.isNull()) {
            return null;
        }
        return Timestamp.valueOf(LocalDateTime.ofInstant(datetimeExpr.timestampValue(), ZoneOffset.UTC));
    }
}
