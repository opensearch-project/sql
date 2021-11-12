/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class PortConnectionProperty extends IntConnectionProperty {

    public static final String KEY = "port";

    public PortConnectionProperty() {
        super(KEY);
    }

    @Override
    protected Integer parseValue(Object value) throws ConnectionPropertyException {
        int intValue = super.parseValue(value);

        if (intValue < 0 || intValue > 65535) {
            throw new ConnectionPropertyException(getKey(),
                    String.format("Port number property requires a valid integer (0-65535). Invalid value: %d", intValue));
        }

        return intValue;
    }

    @Override
    public Integer getDefault() {
        return 9200;
    }
}
