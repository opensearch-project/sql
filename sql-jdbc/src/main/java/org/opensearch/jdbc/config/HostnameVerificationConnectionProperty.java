/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

public class HostnameVerificationConnectionProperty extends BoolConnectionProperty {

    public static final String KEY = "hostnameVerification";

    public HostnameVerificationConnectionProperty() {
        super(KEY);
    }

    @Override
    public Boolean getDefault() {
        return true;
    }
}
