package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;

public class PostprocessForUDTFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Object candidate = args[0];
        SqlTypeName sqlTypeName = (SqlTypeName) args[1];
        Instant instant = InstantUtils.convertToInstant(candidate, sqlTypeName, false);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        switch (sqlTypeName) {
            case DATE:
                return formatDate(localDateTime.toLocalDate());
            case TIME:
                return formatTime(localDateTime.toLocalTime());
            case TIMESTAMP:
                return formatTimestamp(localDateTime);
            default:
                throw new IllegalArgumentException("Unsupported datetime type: " + sqlTypeName);
        }
    }
}
