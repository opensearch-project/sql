package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.time.*;
import java.sql.Timestamp;

public class DateAddFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Mismatch arguments");
        }
        Object arg0 = args[0];
        Object arg1 = args[1];
        Object arg2 = args[2];
        Object arg3 = args[3];

        assert arg0 instanceof TimeUnit;
        assert arg1 instanceof Number;
        assert arg3 instanceof SqlTypeName;
        TimeUnit unit = (TimeUnit) arg0;
        long interval = ((Number) arg1).longValue();
        long base = ((Number) arg2).longValue();
        SqlTypeName sqlTypeName = (SqlTypeName) arg3;
        switch (sqlTypeName){
            case DATE:
                // Convert it to milliseconds
                base *= TimeUnit.DAY.multiplier.longValue();
                break;
            case TIME:
                // Add an offset of today's date at 00:00:00
                LocalDate todayUtc = LocalDate.now(ZoneId.of("UTC"));
                ZonedDateTime startOfDayUtc = todayUtc.atStartOfDay(ZoneId.of("UTC"));
                base += startOfDayUtc.toInstant().toEpochMilli();
            case TIMESTAMP:
                break;
            default:
                throw new IllegalArgumentException("Invalid argument type. Must be DATE, TIME, or TIMESTAMP, but got " + sqlTypeName);
        }

        LocalDateTime newTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(base + unit.multiplier.longValue() * interval), ZoneOffset.UTC);
        return Timestamp.valueOf(newTime);
    }
}
