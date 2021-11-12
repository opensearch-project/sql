/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.util.Map;

public class ByteType extends NumberType<Byte> {
    public static final ByteType INSTANCE = new ByteType();

    private ByteType() {

    }

    public Byte fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return (byte) 0;
        }
        if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof String) {
            return asByte((String) value);
        } else if (value instanceof Number) {
            return asByte((Number) value);
        } else {
            throw objectConversionException(value);
        }
    }

    private Byte asByte(String value) throws SQLException {
        try {
            return asByte(Double.valueOf(value));
        } catch (NumberFormatException nfe) {
            throw stringConversionException(value, nfe);
        }
    }

    private Byte asByte(Number value) throws SQLException {
        return (byte) getDoubleValueWithinBounds(value, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    @Override
    public String getTypeName() {
        return "Byte";
    }
}
