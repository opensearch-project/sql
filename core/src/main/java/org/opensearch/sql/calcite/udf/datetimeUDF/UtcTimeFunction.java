package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.sql.Time;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class UtcTimeFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        var zdt =
                ZonedDateTime.now()
                        .withZoneSameInstant(ZoneOffset.UTC);
        LocalDateTime localDateTime = zdt.toLocalDateTime();
        Time time = new Time(localDateTime.getHour(), localDateTime.getMinute(), localDateTime.getSecond());
        return time;
    }
}
