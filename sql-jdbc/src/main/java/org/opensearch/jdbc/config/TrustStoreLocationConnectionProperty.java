/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class TrustStoreLocationConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "trustStoreLocation";

    public TrustStoreLocationConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
