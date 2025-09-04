/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.directquery.DirectQueryExecutorService;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetDirectQueryResourcesRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private ActionFilters actionFilters;
  @Mock private DirectQueryExecutorServiceImpl mockExecutorService;
  @Mock private DirectQueryExecutorService executorService;
  @Mock private Task task;
  @Mock private GetDirectQueryResourcesActionRequest actionRequest;
  @Mock private GetDirectQueryResourcesRequest directQueryRequest;
  @Mock private ActionListener<GetDirectQueryResourcesActionResponse> actionListener;

  private TransportGetDirectQueryResourcesRequestAction transportAction;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);

    transportAction =
        new TransportGetDirectQueryResourcesRequestAction(
            transportService, actionFilters, mockExecutorService);

    when(actionRequest.getDirectQueryRequest()).thenReturn(directQueryRequest);
  }

  @Test
  public void testDoExecuteSuccess() {
    // Prepare mock response
    GetDirectQueryResourcesResponse mockResponse = new GetDirectQueryResourcesResponse();

    when(mockExecutorService.getDirectQueryResources(any(GetDirectQueryResourcesRequest.class)))
        .thenReturn(mockResponse);

    // Execute the action
    transportAction.doExecute(task, actionRequest, actionListener);

    // Verify correct execution
    verify(mockExecutorService).getDirectQueryResources(directQueryRequest);
    verify(actionListener).onResponse(any(GetDirectQueryResourcesActionResponse.class));
  }

  @Test
  public void testDoExecuteFailure() {
    // Setup to throw exception
    RuntimeException exception = new RuntimeException("Test exception");
    when(mockExecutorService.getDirectQueryResources(any(GetDirectQueryResourcesRequest.class)))
        .thenThrow(exception);

    // Execute the action
    transportAction.doExecute(task, actionRequest, actionListener);

    // Verify exception was passed to listener
    verify(actionListener).onFailure(exception);
  }
}
