package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class UtcTimeStampFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        var zdt =
                ZonedDateTime.now()
                        .withZoneSameInstant(ZoneOffset.UTC);
        return zdt.toInstant().toEpochMilli();
    }
}
