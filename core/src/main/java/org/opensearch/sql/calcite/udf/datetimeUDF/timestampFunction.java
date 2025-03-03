package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;

public class timestampFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        String timestampExpression = (String) args[0];
        LocalDateTime datetime = LocalDateTime.parse(timestampExpression, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
        Instant dateTimeMills = datetime.toInstant(ZoneOffset.UTC);
        Long addTimeMills = 0L;
        if (args.length > 1) {
            Object addTimeExpr = args[1];
            if (addTimeExpr instanceof String) {
                LocalDateTime addTime = LocalDateTime.parse((String) addTimeExpr, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
                addTimeMills = addTime.toLocalTime().toSecondOfDay() * 1000L;
                return addTwoTimestamp(dateTimeMills, addTimeMills);
            }
            else if (addTimeExpr instanceof java.sql.Time) {
                addTimeMills = ((java.util.Date) addTimeExpr).toInstant().toEpochMilli();
                return addTwoTimestamp(dateTimeMills, addTimeMills);
            }
            else if (addTimeExpr instanceof java.sql.Timestamp) {
                addTimeMills  = ((java.sql.Timestamp) addTimeExpr).toLocalDateTime().toLocalTime().toSecondOfDay() * 1000L;
                return addTwoTimestamp(dateTimeMills, addTimeMills);
            }
        }
        return addTwoTimestamp(dateTimeMills, addTimeMills);
    }

    private java.sql.Timestamp addTwoTimestamp(Instant timestamp, Long addTime) {
        Instant newInstant = timestamp.plusMillis(addTime);
        LocalDateTime newTime = LocalDateTime.ofInstant(newInstant, ZoneOffset.UTC);
        return java.sql.Timestamp.valueOf(newTime);
    }
}
