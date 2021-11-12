/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.transport.http;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.logging.Logger;
import org.opensearch.jdbc.transport.TransportException;
import org.opensearch.jdbc.transport.TransportFactory;

public class ApacheHttpTransportFactory implements TransportFactory<ApacheHttpTransport> {

    public static ApacheHttpTransportFactory INSTANCE = new ApacheHttpTransportFactory();

    private ApacheHttpTransportFactory() {

    }

    @Override
    public ApacheHttpTransport getTransport(ConnectionConfig config, Logger log, String userAgent) throws TransportException {
        return new ApacheHttpTransport(config, log, userAgent);
    }
}
