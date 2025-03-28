package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;

public class PostprocessDateToStringFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Object candidate = args[0];
        if (Objects.isNull(candidate)) {
            return null;
        }
        Instant instant = InstantUtils.convertToInstant(candidate, SqlTypeName.VARCHAR, false);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn");
        String formatted = localDateTime.format(formatter);
        return formatted;
    }
}