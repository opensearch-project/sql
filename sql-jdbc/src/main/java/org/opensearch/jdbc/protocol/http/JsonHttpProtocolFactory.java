/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.protocol.ProtocolFactory;
import org.opensearch.jdbc.transport.http.HttpTransport;


public class JsonHttpProtocolFactory implements ProtocolFactory<JsonHttpProtocol, HttpTransport> {

    public static JsonHttpProtocolFactory INSTANCE = new JsonHttpProtocolFactory();

    private JsonHttpProtocolFactory() {

    }

    @Override
    public JsonHttpProtocol getProtocol(ConnectionConfig connectionConfig, HttpTransport transport) {
        return new JsonHttpProtocol(transport);
    }
}
