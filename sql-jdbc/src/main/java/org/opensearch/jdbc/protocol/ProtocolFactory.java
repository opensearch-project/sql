/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.transport.Transport;

public interface ProtocolFactory<P extends Protocol, T extends Transport> {
    P getProtocol(ConnectionConfig config, T transport);
}
