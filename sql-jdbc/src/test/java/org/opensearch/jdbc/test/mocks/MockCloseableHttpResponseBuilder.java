/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.test.mocks;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockCloseableHttpResponseBuilder {

    private int httpCode;
    private String responseBody;
    private Header contentTypeHeader;

    public MockCloseableHttpResponseBuilder withHttpReturnCode(int httpCode) {
        this.httpCode = httpCode;
        return this;
    }

    public MockCloseableHttpResponseBuilder withResponseBody(String responseBody) {
        this.responseBody = responseBody;
        return this;
    }

    public MockCloseableHttpResponseBuilder withContentType(String contentType) {
        this.contentTypeHeader = new BasicHeader("content-type", contentType);
        return this;
    }

    public CloseableHttpResponse build() throws IOException {
        StatusLine mockStatusLine = mock(StatusLine.class);
        HttpEntity mockEntity = mock(HttpEntity.class);

        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(httpCode);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getContentType()).thenReturn(contentTypeHeader);

        // this mimics a real stream that can be consumed just once
        // as is the case with a server response. This makes this mock
        // response object single-use with regards to reading the
        // response content.
        when(mockEntity.getContent()).thenReturn(responseBody == null ? null
                        : new ByteArrayInputStream(responseBody.getBytes("UTF-8")));
        return mockResponse;
    }
}
