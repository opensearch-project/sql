package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.sql.Date;

public class utcTimeFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        var zdt =
                ZonedDateTime.now()
                        .withZoneSameInstant(ZoneOffset.UTC);
        LocalDate dateOnly = zdt.toLocalDate();
        long millisAtStartOfDay = dateOnly.atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        return millisAtStartOfDay;
        //return zdt.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
