/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;
import java.util.Map;

public class BooleanType implements TypeHelper<Boolean> {

    public static final BooleanType INSTANCE = new BooleanType();

    private BooleanType() {

    }
    
    @Override
    public Boolean fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
        if (value == null) {
            return false;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return asBoolean((String) value);
        } else {
            throw objectConversionException(value);
        }
    }

    private Boolean asBoolean(String value) throws SQLException {
        try {
            return Boolean.valueOf(value);
        } catch (NumberFormatException nfe) {
            throw stringConversionException(value, nfe);
        }
    }


    @Override
    public String getTypeName() {
        return "Boolean";
    }
}
