/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.exceptions;

import java.sql.SQLNonTransientException;

public class InternalServerErrorException extends SQLNonTransientException {

    String reason;
    String type;
    String details;

    public InternalServerErrorException(String reason, String type, String details) {
        this.reason = reason;
        this.type = type;
        this.details = details;
    }

    public String getReason() {
        return reason;
    }

    public String getType() {
        return type;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public String toString() {
        return "Internal Server Error. Reason: "+ reason+". " +
                "Type: "+ type+". Details: "+ details;
    }
}
