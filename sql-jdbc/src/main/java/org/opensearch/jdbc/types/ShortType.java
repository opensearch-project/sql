/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.util.Map;

public class ShortType extends NumberType<Short> {

    public static final ShortType INSTANCE = new ShortType();

    private ShortType() {

    }

    @Override
    public Short fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return (short) 0;
        }
        if (value instanceof Short) {
            return (Short) value;
        } else if (value instanceof String) {
            return asShort((String) value);
        } else if (value instanceof Number) {
            return asShort((Number) value);
        } else {
            throw objectConversionException(value);
        }
    }

    private Short asShort(String value) throws SQLException {
        try {
            return asShort(Double.valueOf(value));
        } catch (NumberFormatException nfe) {
            throw stringConversionException(value, nfe);
        }
    }

    private Short asShort(Number value) throws SQLException {
        return (short) getDoubleValueWithinBounds(value, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    @Override
    public String getTypeName() {
        return "Short";
    }

    public static void main(String[] args) {
        Short.valueOf("45.50");
        System.out.println(Math.round(100.45D));
        System.out.println(Math.round(100.95f));
    }
}
