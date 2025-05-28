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
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionRequest;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportExecuteDirectQueryRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private ActionFilters actionFilters;
  @Mock private DirectQueryExecutorServiceImpl mockExecutorService;
  @Mock private DirectQueryExecutorService executorService;
  @Mock private Task task;
  @Mock private ExecuteDirectQueryActionRequest actionRequest;
  @Mock private ExecuteDirectQueryRequest directQueryRequest;
  @Mock private ActionListener<ExecuteDirectQueryActionResponse> actionListener;

  private TransportExecuteDirectQueryRequestAction transportAction;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);

    transportAction =
        new TransportExecuteDirectQueryRequestAction(
            transportService, actionFilters, mockExecutorService);

    when(actionRequest.getDirectQueryRequest()).thenReturn(directQueryRequest);
  }

  @Test
  public void testDoExecuteSuccess() {
    // Prepare mock response with valid data
    ExecuteDirectQueryResponse mockResponse = new ExecuteDirectQueryResponse();
    mockResponse.setQueryId("test-query-id");
    mockResponse.setSessionId("test-session-id");
    mockResponse.setResult(
        "{\"data\":{\"resultType\":\"vector\",\"result\":[]}}"); // Valid Prometheus JSON
    mockResponse.setDataSourceType("prometheus");

    when(mockExecutorService.executeDirectQuery(any(ExecuteDirectQueryRequest.class)))
        .thenReturn(mockResponse);

    // Execute the action
    transportAction.doExecute(task, actionRequest, actionListener);

    // Verify correct execution
    verify(mockExecutorService).executeDirectQuery(directQueryRequest);
    verify(actionListener).onResponse(any(ExecuteDirectQueryActionResponse.class));
  }

  @Test
  public void testDoExecuteFailure() {
    // Setup to throw exception
    RuntimeException exception = new RuntimeException("Test exception");
    when(mockExecutorService.executeDirectQuery(any(ExecuteDirectQueryRequest.class)))
        .thenThrow(exception);

    // Execute the action
    transportAction.doExecute(task, actionRequest, actionListener);

    // Verify exception was passed to listener
    verify(actionListener).onFailure(exception);
  }
}
