/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class UserConnectionProperty extends StringConnectionProperty {

    public static final String KEY = "user";

    public UserConnectionProperty() {
        super(KEY);
    }

}
