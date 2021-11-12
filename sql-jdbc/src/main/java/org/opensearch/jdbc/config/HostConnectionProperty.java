/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class HostConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "host";

    public HostConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return "localhost";
    }

}
