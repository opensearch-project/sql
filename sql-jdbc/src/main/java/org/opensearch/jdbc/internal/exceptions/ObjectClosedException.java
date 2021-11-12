/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.internal.exceptions;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * Exception indicating JDBC operation can not occur due to the
 * target object being in Closed state
 */
public class ObjectClosedException extends SQLNonTransientException {

    public ObjectClosedException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public ObjectClosedException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public ObjectClosedException(String reason) {
        super(reason);
    }

    public ObjectClosedException() {
    }

    public ObjectClosedException(Throwable cause) {
        super(cause);
    }
}
