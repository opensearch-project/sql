/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class KeyStorePasswordConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "keyStorePassword";

    public KeyStorePasswordConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
