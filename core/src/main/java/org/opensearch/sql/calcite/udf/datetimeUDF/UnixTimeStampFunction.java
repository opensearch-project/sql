package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class UnixTimeStampFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length == 0) {
            LocalDateTime localDateTime = LocalDateTime.now();
            return localDateTime.toEpochSecond(ZoneOffset.UTC);
        }
        Object input = args[0];
        String inputTypes = args[1].toString();
        if (input instanceof Number) {
            // return null
            // what if calcite sql timestamp?
            // number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format.
            if (inputTypes.equals("double")) {
                return null;
            } else {
                LocalDateTime localDateTime = Instant.ofEpochMilli(((long) input)).atZone(ZoneId.systemDefault()).toLocalDateTime();
                return localDateTime.toEpochSecond(ZoneOffset.UTC);
            }
        }
        else if (input instanceof java.sql.Date) {
            LocalDate localDate = ((java.sql.Date) input).toLocalDate();
            return localDate.toEpochSecond(LocalTime.MIN, ZoneOffset.UTC);
        }
        else if (input instanceof java.sql.Timestamp) {
            LocalDateTime localDateTime = ((java.sql.Timestamp) input).toLocalDateTime();
            return localDateTime.toEpochSecond(ZoneOffset.UTC);
        }

        return null;


    }
}
