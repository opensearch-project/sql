/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class IntConnectionProperty extends ConnectionProperty<Integer> {

    public IntConnectionProperty(String key) {
        super(key);
    }

    @Override
    protected Integer parseValue(Object value) throws ConnectionPropertyException {

        if (value == null) {
            return getDefault();
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException nfe) {
                // invalid value
            }
        }

        throw new ConnectionPropertyException(getKey(),
                String.format("Property %s requires a valid integer. Invalid property value %s. ", getKey(), value));
    }

    @Override
    public Integer getDefault() {
        return 0;
    }
}
