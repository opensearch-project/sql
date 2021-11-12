/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.exceptions.ResponseException;

/**
 * Exception thrown when an unexpected HTTP response code is
 * received from the server.
 */
public class HttpException extends ResponseException {

    private int statusCode;

    /**
     * @param statusCode HTTP Status code due to which this exception is raised.
     * @param message Message associated with the exception - can be the HTTP
     *         reason phrase corresponding to the status code.
     */
    public HttpException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public HttpException(int statusCode, String message, Throwable cause, String responsePayload) {
        super(message, cause, responsePayload);
        this.statusCode = statusCode;
    }

    /**
     * Returns the HTTP response status code that resulted in
     * this exception.
     *
     * @return HTTP status code
     */
    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String getLocalizedMessage() {
        String message = getMessage();
        String localizedMessage = "HTTP Code: " + statusCode +
                ". Message: " + (message == null ? "None" : message) + ".";

        if (this.getResponsePayload() != null) {
            localizedMessage += " Raw response received: " + getResponsePayload();
        }

        return localizedMessage;
    }
}
