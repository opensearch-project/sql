/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.Map;

public class LongType extends NumberType<Long> {

    public static final LongType INSTANCE = new LongType();

    private LongType() {

    }

    @Override
    public Long fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return (long) 0;
        }
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            return asLong((String) value);
        } else if (value instanceof Number) {
            return asLong((Number) value);
        } else {
            throw objectConversionException(value);
        }
    }

    private Long asLong(String value) throws SQLException {
        try {
            if (value.length() > 14) {
                // more expensive conversion but
                // needed to preserve precision for such large numbers
                BigDecimal bd = new BigDecimal(value);
                bd = bd.setScale(0, RoundingMode.HALF_UP);
                return bd.longValueExact();
            } else {
                return asLong(Double.valueOf(value));
            }

        } catch (ArithmeticException | NumberFormatException ex) {
            throw stringConversionException(value, ex);
        }
    }

    private Long asLong(Number value) throws SQLException {
        return (long) getDoubleValueWithinBounds(value, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Override
    public String getTypeName() {
        return "Long";
    }
}
