/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.transport.http;

import org.opensearch.jdbc.transport.Transport;
import org.opensearch.jdbc.transport.TransportException;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;

public interface HttpTransport extends Transport {

    CloseableHttpResponse doGet(String path, Header[] headers, HttpParam[] params, int timeout)
            throws TransportException;

    CloseableHttpResponse doPost(String path, Header[] headers, HttpParam[] params, String body, int timeout)
            throws TransportException;
}
