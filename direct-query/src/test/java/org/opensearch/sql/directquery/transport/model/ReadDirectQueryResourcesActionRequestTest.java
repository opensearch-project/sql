/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;

public class ReadDirectQueryResourcesActionRequestTest {

    @Test
    public void testConstructorWithRequest() {
        GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
        request.setDataSource("prometheus");
        request.setResourceType(DirectQueryResourceType.LABELS);

        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(request);

        assertNotNull(actionRequest.getDirectQueryRequest());
        assertEquals("prometheus", actionRequest.getDirectQueryRequest().getDataSource());
        assertEquals(DirectQueryResourceType.LABELS, actionRequest.getDirectQueryRequest().getResourceType());
    }

    @Test
    public void testStreamConstructorWithAllFields() throws IOException {
        StreamInput streamInput = mock(StreamInput.class);
        Map<Object, Object> queryParams = new HashMap<>();
        queryParams.put("metric", "up");

        when(streamInput.readOptionalString())
            .thenReturn("prometheus")  // dataSource
            .thenReturn("LABELS")      // resourceType
            .thenReturn("test_metric"); // resourceName

        when(streamInput.readMap(any(), any())).thenReturn(queryParams);

        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(streamInput);

        assertNotNull(actionRequest.getDirectQueryRequest());
        assertEquals("prometheus", actionRequest.getDirectQueryRequest().getDataSource());
        assertEquals(DirectQueryResourceType.LABELS, actionRequest.getDirectQueryRequest().getResourceType());
        assertEquals("test_metric", actionRequest.getDirectQueryRequest().getResourceName());
        assertEquals(queryParams, actionRequest.getDirectQueryRequest().getQueryParams());
    }

    @Test
    public void testStreamConstructorWithNullResourceType() throws IOException {
        StreamInput streamInput = mock(StreamInput.class);
        Map<Object, Object> queryParams = new HashMap<>();

        when(streamInput.readOptionalString())
            .thenReturn("prometheus")  // dataSource
            .thenReturn(null)          // resourceType
            .thenReturn("test_metric"); // resourceName

        when(streamInput.readMap(any(), any())).thenReturn(queryParams);

        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(streamInput);

        assertNotNull(actionRequest.getDirectQueryRequest());
        assertEquals("prometheus", actionRequest.getDirectQueryRequest().getDataSource());
        assertNull(actionRequest.getDirectQueryRequest().getResourceType());
        assertEquals("test_metric", actionRequest.getDirectQueryRequest().getResourceName());
    }

    @Test
    public void testWriteToWithAllFields() throws IOException {
        GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
        request.setDataSource("prometheus");
        request.setResourceType(DirectQueryResourceType.METADATA);
        request.setResourceName("cpu_usage");

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("filter", "instance");
        request.setQueryParams(queryParams);

        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(request);
        StreamOutput streamOutput = mock(StreamOutput.class);

        actionRequest.writeTo(streamOutput);

        verify(streamOutput).writeOptionalString("prometheus");
        verify(streamOutput).writeOptionalString("METADATA");
        verify(streamOutput).writeOptionalString("cpu_usage");
        verify(streamOutput).writeMap(any(), any(), any());
    }

    @Test
    public void testWriteToWithNullResourceType() throws IOException {
        GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
        request.setDataSource("prometheus");
        request.setResourceType(null);
        request.setResourceName("cpu_usage");
        request.setQueryParams(new HashMap<>());

        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(request);
        StreamOutput streamOutput = mock(StreamOutput.class);

        actionRequest.writeTo(streamOutput);

        verify(streamOutput).writeOptionalString("prometheus");
        verify(streamOutput).writeOptionalString(null);
        verify(streamOutput).writeOptionalString("cpu_usage");
        verify(streamOutput).writeMap(any(), any(), any());
    }

    @Test
    public void testValidate() {
        GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(request);

        assertNull(actionRequest.validate());
    }

    @Test
    public void testGetDirectQueryRequest() {
        GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
        request.setDataSource("test");
        ReadDirectQueryResourcesActionRequest actionRequest = new ReadDirectQueryResourcesActionRequest(request);

        GetDirectQueryResourcesRequest retrievedRequest = actionRequest.getDirectQueryRequest();
        assertEquals("test", retrievedRequest.getDataSource());
        assertEquals(request, retrievedRequest);
    }
}