/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

public class UnrecognizedOpenSearchTypeException extends IllegalArgumentException {

    public UnrecognizedOpenSearchTypeException() {
    }

    public UnrecognizedOpenSearchTypeException(String s) {
        super(s);
    }

    public UnrecognizedOpenSearchTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecognizedOpenSearchTypeException(Throwable cause) {
        super(cause);
    }
}
