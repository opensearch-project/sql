/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.config;

/**
 * The Path connection property.
 *
 * A trailing '/' is not expected or required in the
 * input value but is ignored if present.
 *
 */
public class PathConnectionProperty extends StringConnectionProperty {

    public static final String KEY = "path";

    public PathConnectionProperty() {
        super(KEY);
    }

    @Override
    protected String parseValue(Object value) throws ConnectionPropertyException {
        String stringValue = super.parseValue(value);

        // Remove the trailing '/' as all internal calls
        // will implicitly apply this.
        if (stringValue.length() > 1 && stringValue.endsWith("/")) {
            stringValue = stringValue.substring(0, stringValue.length()-1);
        }
        return stringValue;
    }

    @Override
    public String getDefault() {
        return "";
    }
}
