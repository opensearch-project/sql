/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class KeyStoreTypeConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "keyStoreType";

    public KeyStoreTypeConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return "JKS";
    }

}
