/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportSqlQueryActionTest {

  private TransportSqlQueryAction action;

  @Before
  public void setUp() {
    TransportService transportService = mock(TransportService.class);
    action = new TransportSqlQueryAction(transportService, new ActionFilters(new HashSet<>()));
  }

  @After
  public void tearDown() {
    // doExecute binds the task to a ThreadLocal and does not clear it on the caller thread.
    OpenSearchQueryManager.clearCancellableTask();
  }

  private SqlQueryTask newTask() {
    return new SqlQueryTask(
        1, "transport", SqlQueryAction.NAME, "SELECT 1", TaskId.EMPTY_TASK_ID, Map.of());
  }

  @SuppressWarnings("unchecked")
  private ActionListener<TransportSqlQueryResponse> mockListener() {
    return mock(ActionListener.class);
  }

  @Test
  public void completesListenerOnceWhenWorkSendsResponse() {
    RestChannel delegate = mock(RestChannel.class);
    RestResponse response = mock(RestResponse.class);
    Consumer<RestChannel> work = ch -> ch.sendResponse(response);
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", work, delegate);
    ActionListener<TransportSqlQueryResponse> listener = mockListener();

    action.doExecute(newTask(), request, listener);

    verify(delegate, times(1)).sendResponse(response);
    verify(listener, times(1)).onResponse(any(TransportSqlQueryResponse.class));
    verify(listener, never()).onFailure(any());
  }

  @Test
  public void bindsCancellableTaskDuringExecution() {
    SqlQueryTask task = newTask();
    RestChannel delegate = mock(RestChannel.class);
    RestResponse response = mock(RestResponse.class);
    AtomicReference<Task> seen = new AtomicReference<>();
    Consumer<RestChannel> work =
        ch -> {
          seen.set(OpenSearchQueryManager.getCancellableTask());
          ch.sendResponse(response);
        };
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", work, delegate);

    action.doExecute(task, request, mockListener());

    assertSame(task, seen.get());
  }

  @Test
  public void reportsFailureWhenWorkThrowsBeforeResponse() {
    RestChannel delegate = mock(RestChannel.class);
    RuntimeException boom = new RuntimeException("boom");
    Consumer<RestChannel> work =
        ch -> {
          throw boom;
        };
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", work, delegate);
    ActionListener<TransportSqlQueryResponse> listener = mockListener();

    action.doExecute(newTask(), request, listener);

    verify(listener, times(1)).onFailure(boom);
    verify(listener, never()).onResponse(any());
  }

  @Test
  public void doesNotDoubleCompleteWhenWorkThrowsAfterResponse() {
    RestChannel delegate = mock(RestChannel.class);
    RestResponse response = mock(RestResponse.class);
    Consumer<RestChannel> work =
        ch -> {
          ch.sendResponse(response);
          throw new RuntimeException("late failure after response already sent");
        };
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", work, delegate);
    ActionListener<TransportSqlQueryResponse> listener = mockListener();

    action.doExecute(newTask(), request, listener);

    verify(listener, times(1)).onResponse(any(TransportSqlQueryResponse.class));
    verify(listener, never()).onFailure(any());
  }

  @Test
  public void secondSendResponseDoesNotCompleteListenerTwice() {
    RestChannel delegate = mock(RestChannel.class);
    RestResponse first = mock(RestResponse.class);
    RestResponse second = mock(RestResponse.class);
    Consumer<RestChannel> work =
        ch -> {
          ch.sendResponse(first);
          ch.sendResponse(second);
        };
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", work, delegate);
    ActionListener<TransportSqlQueryResponse> listener = mockListener();

    action.doExecute(newTask(), request, listener);

    verify(listener, times(1)).onResponse(any(TransportSqlQueryResponse.class));
  }
}
