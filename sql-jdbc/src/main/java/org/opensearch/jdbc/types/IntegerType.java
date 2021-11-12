/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.util.Map;

public class IntegerType extends NumberType<Integer> {

    public static final IntegerType INSTANCE = new IntegerType();

    private IntegerType() {

    }

    @Override
    public Integer fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return asInteger((String) value);
        } else if (value instanceof Number) {
            return asInteger((Number) value);
        } else {
            throw objectConversionException(value);
        }
    }

    private Integer asInteger(String value) throws SQLException {
        try {
            return asInteger(Double.valueOf(value));
        } catch (NumberFormatException nfe) {
            throw stringConversionException(value, nfe);
        }
    }

    private Integer asInteger(Number value) throws SQLException {
        return (int) getDoubleValueWithinBounds(value, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Override
    public String getTypeName() {
        return "Integer";
    }

}
