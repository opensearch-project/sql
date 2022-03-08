/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * Supports returning a java.sql.Timestamp from a String in the
 * JDBC escape format, or a Number value indicating epoch time in millis.
 */
public class TimestampType implements TypeHelper<Timestamp> {

    public static final TimestampType INSTANCE = new TimestampType();

    private TimestampType() {

    }

    @Override
    public java.sql.Timestamp fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return null;
        }
        Calendar calendar = conversionParams != null ? (Calendar) conversionParams.get("calendar") : null;
        if (value instanceof Timestamp) {
            return asTimestamp((Timestamp) value, calendar);
        } else if (value instanceof String) {
            return asTimestamp((String) value, calendar);
        } else if (value instanceof Number) {
           return asTimestamp((Number) value);
        } else {
            throw objectConversionException(value);
        }
    }

    public java.sql.Timestamp asTimestamp(Timestamp value, Calendar calendar) throws SQLException {
        if (calendar == null) {
            return value;
        } else {
            return localDateTimeToTimestamp(value.toLocalDateTime(), calendar);
        }
    }

    private Timestamp localDateTimeToTimestamp(LocalDateTime ldt, Calendar calendar) {
        calendar.set(ldt.getYear(), ldt.getMonthValue()-1, ldt.getDayOfMonth(),
                ldt.getHour(), ldt.getMinute(), ldt.getSecond());
        calendar.set(Calendar.MILLISECOND, ldt.getNano()/1000000);

        return new Timestamp(calendar.getTimeInMillis());
    }

    public java.sql.Timestamp asTimestamp(String value, Calendar calendar) throws SQLException {
        try {
            // Make some effort to understand ISO format
            if (value.length() > 11 && value.charAt(10) == 'T') {
                value = value.replace('T', ' ');
            }
            // Timestamp.valueOf() does not like timezone information
            if (value.length() > 23) {
                if (value.length() == 24 && value.charAt(23) == 'Z') {
                    value = value.substring(0, 23);
                }
                else if (value.charAt(23) == '+' || value.charAt(23) == '-') {
                    // 'calendar' parameter takes precedence
                    if (calendar == null) {
                        calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT" + value.substring(23)));
                    }
                    value = value.substring(0, 23);
                }
            }

            final Timestamp ts;
            // 11 to check if the value is in yyyy-MM-dd format
            if (value.length() < 11) {
                ts = Timestamp.valueOf(LocalDate.parse(value).atStartOfDay());
            } else {
                ts = Timestamp.valueOf(value);
            }

            if (calendar == null) {
                return ts;
            }
            return localDateTimeToTimestamp(ts.toLocalDateTime(), calendar);
        } catch (IllegalArgumentException iae) {
            throw stringConversionException(value, iae);
        }
    }

    public java.sql.Timestamp asTimestamp(Number value) throws SQLException {
        return new java.sql.Timestamp(value.longValue());
    }

    @Override
    public String getTypeName() {
        return "Timestamp";
    }

}
