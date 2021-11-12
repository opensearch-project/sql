/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

import java.sql.SQLException;

public class ConnectionPropertyException extends SQLException {

    String propertyKey;

    public ConnectionPropertyException(String key) {
        super();
        this.propertyKey = key;
    }

    public ConnectionPropertyException(String key, String message) {
        super(message);
        this.propertyKey = key;
    }

    public ConnectionPropertyException(String key, String message, Throwable cause) {
        super(message, cause);
        this.propertyKey = key;
    }

    public ConnectionPropertyException(String key, Throwable cause) {
        super(cause);
        this.propertyKey = key;
    }

    public String getPropertyKey() {
        return propertyKey;
    }
}
