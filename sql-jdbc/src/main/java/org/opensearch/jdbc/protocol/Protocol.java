/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

import org.opensearch.jdbc.protocol.exceptions.ResponseException;

import java.io.IOException;

public interface Protocol extends AutoCloseable {

    ConnectionResponse connect(int timeout) throws ResponseException, IOException;

    QueryResponse execute(QueryRequest request) throws ResponseException, IOException;

    void close() throws IOException;
}
