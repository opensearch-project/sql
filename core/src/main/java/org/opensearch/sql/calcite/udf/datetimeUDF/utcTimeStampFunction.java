package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.sql.Date;

public class utcTimeStampFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        var zdt =
                ZonedDateTime.now()
                        .withZoneSameInstant(ZoneOffset.UTC);
        Long milli = zdt.toInstant().toEpochMilli();
        Timestamp timestamp = new Timestamp(milli);
        return zdt.toInstant().toEpochMilli();
    }
}
