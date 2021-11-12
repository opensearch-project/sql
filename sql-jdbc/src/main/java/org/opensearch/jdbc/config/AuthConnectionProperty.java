/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

import org.opensearch.jdbc.auth.AuthenticationType;

import java.util.Locale;

public class AuthConnectionProperty extends ConnectionProperty<AuthenticationType> {

    public static final String KEY = "auth";

    public AuthConnectionProperty() {
        super(KEY);
    }

    @Override
    protected AuthenticationType parseValue(Object rawValue) throws ConnectionPropertyException {
        if (rawValue == null) {
            return getDefault();
        } else if (rawValue instanceof String) {
            String stringValue = (String) rawValue;
            try {
                return AuthenticationType.valueOf(stringValue.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                throw new ConnectionPropertyException(getKey(),
                        String.format("Invalid value specified for the property \"%s\". " +
                                "Unknown authentication type \"%s\".", getKey(), stringValue));
            }
        }

        throw new ConnectionPropertyException(getKey(),
                String.format("Property \"%s\" requires a valid String matching a known authentication type. " +
                        "Invalid value of type: %s specified.", getKey(), rawValue.getClass().getName()));

    }

    @Override
    public AuthenticationType getDefault() {
        return AuthenticationType.NONE;
    }
}
