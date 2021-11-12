/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.protocol.ProtocolFactory;
import org.opensearch.jdbc.transport.http.HttpTransport;

/**
 * Factory to create JsonCursorHttpProtocol objects
 *
 *  @author abbas hussain
 *  @since 07.05.20
 */
public class JsonCursorHttpProtocolFactory implements ProtocolFactory<JsonCursorHttpProtocol, HttpTransport> {

    public static JsonCursorHttpProtocolFactory INSTANCE = new JsonCursorHttpProtocolFactory();

    private JsonCursorHttpProtocolFactory() {

    }

    @Override
    public JsonCursorHttpProtocol getProtocol(ConnectionConfig connectionConfig, HttpTransport transport) {
        return new JsonCursorHttpProtocol(transport);
    }
}
