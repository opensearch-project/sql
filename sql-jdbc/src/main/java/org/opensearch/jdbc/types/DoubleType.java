/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.util.Map;

public class DoubleType implements TypeHelper<Double> {

    public static final DoubleType INSTANCE = new DoubleType();

    private DoubleType() {

    }

    @Override
    public Double fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return (double) 0;
        }
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof String) {
            return asDouble((String) value);
        } else if (value instanceof Number) {
            return asDouble((Number) value);
        } else {
            throw objectConversionException(value);
        }
    }

    private Double asDouble(String value) throws SQLException {
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException nfe) {
            throw stringConversionException(value, nfe);
        }
    }

    private Double asDouble(Number value) throws SQLException {
        return value.doubleValue();
    }

    @Override
    public String getTypeName() {
        return "Double";
    }
}
