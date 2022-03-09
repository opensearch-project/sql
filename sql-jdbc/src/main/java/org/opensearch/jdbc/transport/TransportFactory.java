/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.transport;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.logging.Logger;

public interface TransportFactory<T extends Transport> {

    T getTransport(ConnectionConfig config, Logger log, String userAgent) throws TransportException;
}
