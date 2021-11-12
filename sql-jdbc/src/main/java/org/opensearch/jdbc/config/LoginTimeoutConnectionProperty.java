/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

/**
 * Login / Read timeout in seconds
 */
public class LoginTimeoutConnectionProperty extends IntConnectionProperty {

    public static final String KEY = "loginTimeout";

    public LoginTimeoutConnectionProperty() {
        super(KEY);
    }

    @Override
    protected Integer parseValue(Object value) throws ConnectionPropertyException {
        int intValue = super.parseValue(value);

        if (intValue < 0) {
            throw new ConnectionPropertyException(getKey(),
                    String.format("Login timeout property requires a valid integer >=0. Invalid value: %d", intValue));
        }
        return intValue;
    }

}
