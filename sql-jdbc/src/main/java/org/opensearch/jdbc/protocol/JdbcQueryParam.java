/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

public class JdbcQueryParam implements Parameter {
    private Object value;

    private String type;

    public JdbcQueryParam(String type, Object value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public String getType() {
        return type;
    }
}
