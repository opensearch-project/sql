/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class TrustStorePasswordConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "trustStorePassword";

    public TrustStorePasswordConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
