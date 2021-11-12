/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.exceptions.ResponseException;
import org.apache.http.HttpResponse;

public interface HttpResponseHandler<T> {

    T handleResponse(HttpResponse response) throws ResponseException;
}
