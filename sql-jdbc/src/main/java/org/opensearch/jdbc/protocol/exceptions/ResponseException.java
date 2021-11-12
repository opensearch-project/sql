/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.exceptions;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Exception thrown when an unexpected server response is received.
 */
public class ResponseException extends Exception {
    private String responsePayload = null;

    public ResponseException() {
    }

    public ResponseException(String message) {
        super(message);
    }

    public ResponseException(String message, Throwable cause, String responsePayload) {
        super(message, cause);
        this.responsePayload = responsePayload;
    }

    public ResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResponseException(Throwable cause) {
        super(cause);
    }

    public String getResponsePayload() {
        return responsePayload;
    }

    @Override
    public String getLocalizedMessage() {
        String localizedMessage = super.getLocalizedMessage();

        if (responsePayload != null) {
            localizedMessage = (localizedMessage == null ? "" : localizedMessage)
                    + " Raw response received: " + getResponsePayload();
        }

        return localizedMessage;
    }
}
