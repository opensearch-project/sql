/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class TrustStoreTypeConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "trustStoreType";

    public TrustStoreTypeConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return "JKS";
    }

}
