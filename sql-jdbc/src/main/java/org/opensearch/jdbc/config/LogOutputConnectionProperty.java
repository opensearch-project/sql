/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class LogOutputConnectionProperty extends StringConnectionProperty {

    public static final String KEY = "logOutput";

    public LogOutputConnectionProperty() {
        super(KEY);
    }

}
