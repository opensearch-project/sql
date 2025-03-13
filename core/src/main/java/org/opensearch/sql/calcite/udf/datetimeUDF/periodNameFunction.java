package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.common.utils.StringUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * We cannot use dayname/monthname in calcite because they're different with our current performance
 * e.g. August -> Aug, Wednesday -> Wed
 */
public class periodNameFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Object candiate = args[0];
        Object type = args[1];
        SqlTypeName argumentType = (SqlTypeName) args[2];
        LocalDate localDate;
        if (candiate instanceof String) {
            // First transfer it to LocalDate
            localDate = LocalDate.parse((String) candiate);
        } else if (argumentType == SqlTypeName.DATE) { // date
            localDate = LocalDate.ofInstant(InstantUtils.fromInternalDate((int) candiate), ZoneOffset.UTC);
        } else if (argumentType == SqlTypeName.TIMESTAMP) { // timestamp
            localDate = LocalDateTime.ofInstant(InstantUtils.fromEpochMills((long) candiate), ZoneOffset.UTC).toLocalDate();
        } else {
            throw new IllegalArgumentException("something wrong");
        }
        String nameType = (String) type;
        if (Objects.equals(nameType, "MONTHNAME")) {
            return localDate.getMonth().getDisplayName(TextStyle.FULL, Locale.getDefault());
        } else if (Objects.equals(nameType, "DAYNAME")) {
            return localDate.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.getDefault());
        } else {
            throw new IllegalArgumentException("something wrong");
        }

    }
}
