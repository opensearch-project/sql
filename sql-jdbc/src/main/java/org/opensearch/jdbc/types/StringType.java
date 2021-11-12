/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.util.Map;

public class StringType implements TypeHelper<String> {

    public static final StringType INSTANCE = new StringType();

    private StringType() {

    }

    @Override
    public String getTypeName() {
        return "String";
    }

    @Override
    public String fromValue(Object value, Map<String, Object> conversionParams) {
        if (value == null)
            return null;
        else
            return String.valueOf(value);
    }
}
