package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class utcDateFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        var zdt =
                ZonedDateTime.now()
                        .withZoneSameInstant(ZoneOffset.UTC);
        LocalDate dateOnly = zdt.toLocalDate();
        long millisAtStartOfDay = dateOnly.atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        Date date = new Date(millisAtStartOfDay);
        return date;
    }
}
