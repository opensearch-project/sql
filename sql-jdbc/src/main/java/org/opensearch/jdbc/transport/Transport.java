/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.transport;

public interface Transport {

    void close() throws TransportException;

    void setReadTimeout(int timeout);

}
