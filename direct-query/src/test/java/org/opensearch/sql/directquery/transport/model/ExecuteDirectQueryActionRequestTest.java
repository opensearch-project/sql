/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;

public class ExecuteDirectQueryActionRequestTest {

    @Test
    public void testConstructorWithRequest() {
        ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
        request.setDataSources("test-datasource");

        ExecuteDirectQueryActionRequest actionRequest = new ExecuteDirectQueryActionRequest(request);

        assertNotNull(actionRequest.getDirectQueryRequest());
        assertEquals("test-datasource", actionRequest.getDirectQueryRequest().getDataSources());
    }

    @Test
    public void testStreamConstructor() throws IOException {
        StreamInput streamInput = mock(StreamInput.class);

        ExecuteDirectQueryActionRequest actionRequest = new ExecuteDirectQueryActionRequest(streamInput);

        assertNotNull(actionRequest.getDirectQueryRequest());
    }

    @Test
    public void testValidate() {
        ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
        ExecuteDirectQueryActionRequest actionRequest = new ExecuteDirectQueryActionRequest(request);

        assertNull(actionRequest.validate());
    }

    @Test
    public void testWriteTo() throws IOException {
        ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
        ExecuteDirectQueryActionRequest actionRequest = new ExecuteDirectQueryActionRequest(request);
        StreamOutput streamOutput = mock(StreamOutput.class);

        // This should not throw an exception
        actionRequest.writeTo(streamOutput);
    }

    @Test
    public void testGetDirectQueryRequest() {
        ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
        request.setQuery("up");
        ExecuteDirectQueryActionRequest actionRequest = new ExecuteDirectQueryActionRequest(request);

        ExecuteDirectQueryRequest retrievedRequest = actionRequest.getDirectQueryRequest();
        assertEquals("up", retrievedRequest.getQuery());
    }
}