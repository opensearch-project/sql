/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.ClusterMetadata;
import org.opensearch.jdbc.protocol.ConnectionResponse;
import org.opensearch.jdbc.protocol.Protocol;
import org.opensearch.jdbc.protocol.QueryRequest;
import org.opensearch.jdbc.protocol.QueryResponse;
import org.opensearch.jdbc.transport.http.HttpParam;
import org.opensearch.jdbc.transport.http.HttpTransport;
import org.opensearch.jdbc.protocol.exceptions.ResponseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class JsonHttpProtocol implements Protocol {

    // the value is based on the API endpoint the sql plugin sets up,
    // but this could be made configurable if required
    public static final String DEFAULT_SQL_CONTEXT_PATH = "/_plugins/_sql";

    private static final Header acceptJson = new BasicHeader(HttpHeaders.ACCEPT, "application/json");
    private static final Header contentTypeJson = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    private static final HttpParam requestJdbcFormatParam = new HttpParam("format", "jdbc");
    protected static final Header[] defaultJsonHeaders = new Header[]{acceptJson, contentTypeJson};
    private static final Header[] defaultEmptyRequestBodyJsonHeaders = new Header[]{acceptJson};
    protected static final HttpParam[] defaultJdbcParams = new HttpParam[]{requestJdbcFormatParam};

    protected static final ObjectMapper mapper = new ObjectMapper();
    private String sqlContextPath;
    private HttpTransport transport;
    private JsonHttpResponseHandler jsonHttpResponseHandler;

    public JsonHttpProtocol(HttpTransport transport) {
        this(transport, DEFAULT_SQL_CONTEXT_PATH);
    }

    public JsonHttpProtocol(HttpTransport transport, String sqlContextPath) {
        this.transport = transport;
        this.sqlContextPath = sqlContextPath;
        this.jsonHttpResponseHandler = new JsonHttpResponseHandler(this);
    }

    public String getSqlContextPath() {
        return sqlContextPath;
    }

    public HttpTransport getTransport() {
        return this.transport;
    }

    public JsonHttpResponseHandler getJsonHttpResponseHandler() {
        return this.jsonHttpResponseHandler;
    }

    @Override
    public ConnectionResponse connect(int timeout) throws ResponseException, IOException {
        try (CloseableHttpResponse response = transport.doGet(
                "/",
                defaultEmptyRequestBodyJsonHeaders,
                null, timeout)) {

            return jsonHttpResponseHandler.handleResponse(response, this::processConnectionResponse);

        }
    }

    @Override
    public QueryResponse execute(QueryRequest request) throws ResponseException, IOException {
        try (CloseableHttpResponse response = transport.doPost(
                sqlContextPath,
                defaultJsonHeaders,
                defaultJdbcParams,
                buildQueryRequestBody(request), 0)) {

            return jsonHttpResponseHandler.handleResponse(response, this::processQueryResponse);

        }
    }

    private String buildQueryRequestBody(QueryRequest queryRequest) throws IOException {
        JsonQueryRequest jsonQueryRequest = new JsonQueryRequest(queryRequest);
        String requestBody = mapper.writeValueAsString(jsonQueryRequest);
        return requestBody;
    }

    @Override
    public void close() throws IOException {
        this.transport.close();
    }

    private JsonConnectionResponse processConnectionResponse(InputStream contentStream) throws IOException {
        ClusterMetadata clusterMetadata = mapper.readValue(contentStream, JsonClusterMetadata.class);
        return new JsonConnectionResponse(clusterMetadata);
    }

    private JsonQueryResponse processQueryResponse(InputStream contentStream) throws IOException {
        return mapper.readValue(contentStream, JsonQueryResponse.class);
    }
}
