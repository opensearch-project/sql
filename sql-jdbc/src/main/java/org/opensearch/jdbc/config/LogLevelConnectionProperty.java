/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

import org.opensearch.jdbc.logging.LogLevel;

import java.util.Locale;

public class LogLevelConnectionProperty extends ConnectionProperty<LogLevel> {

    public static final String KEY = "logLevel";

    public LogLevelConnectionProperty() {
        super(KEY);
    }

    @Override
    protected LogLevel parseValue(Object rawValue) throws ConnectionPropertyException {
        if (rawValue == null) {
            return getDefault();
        } else if (rawValue instanceof String) {
            String stringValue = (String) rawValue;
            try {
                return LogLevel.valueOf(stringValue.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                throw new ConnectionPropertyException(getKey(),
                        String.format("Invalid value specified for the property \"%s\". " +
                                "Unknown log level \"%s\".", getKey(), stringValue));
            }
        }

        throw new ConnectionPropertyException(getKey(),
                String.format("Property \"%s\" requires a valid String matching a known log level. " +
                        "Invalid value of type: %s specified.", getKey(), rawValue.getClass().getName()));
    }

    @Override
    public LogLevel getDefault() {
        return LogLevel.OFF;
    }
}
