/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.exceptions;

/**
 * Exception thrown when an malformed response is received from the
 * server.
 */
public class MalformedResponseException extends ResponseException {

    public MalformedResponseException() {
    }

    public MalformedResponseException(String message) {
        super(message);
    }

    public MalformedResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedResponseException(Throwable cause) {
        super(cause);
    }

}
