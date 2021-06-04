/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.QueryRequest;
import org.opensearch.jdbc.protocol.QueryResponse;
import org.opensearch.jdbc.protocol.exceptions.ResponseException;
import org.opensearch.jdbc.transport.http.HttpTransport;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * Http protocol for cursor request and response
 *
 *  @author abbas hussain
 *  @since 07.05.20
 **/
public class JsonCursorHttpProtocol extends JsonHttpProtocol {

    public JsonCursorHttpProtocol(HttpTransport transport) {
        this(transport, DEFAULT_SQL_CONTEXT_PATH);
    }

    public JsonCursorHttpProtocol(HttpTransport transport, String sqlContextPath) {
        super(transport, sqlContextPath);
    }

    @Override
    public QueryResponse execute(QueryRequest request) throws ResponseException, IOException {
        try (CloseableHttpResponse response = getTransport().doPost(
                getSqlContextPath(),
                defaultJsonHeaders,
                defaultJdbcParams,
                buildQueryRequestBody(request), 0)) {

            return getJsonHttpResponseHandler().handleResponse(response, this::processQueryResponse);

        }
    }

    private String buildQueryRequestBody(QueryRequest queryRequest) throws IOException {
        JsonCursorQueryRequest jsonQueryRequest = new JsonCursorQueryRequest(queryRequest);
        String requestBody = mapper.writeValueAsString(jsonQueryRequest);
        return requestBody;
    }

    private JsonQueryResponse processQueryResponse(InputStream contentStream) throws IOException {
        return mapper.readValue(contentStream, JsonQueryResponse.class);
    }

}
