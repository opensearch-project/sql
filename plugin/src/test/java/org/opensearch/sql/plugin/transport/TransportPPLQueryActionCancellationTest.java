/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.job.Principal;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.QueryJobService;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.QueryRunner;
import org.opensearch.sql.job.SecurityAdapter;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Covers the task-to-runner cancellation contract without constructing the plugin's Guice graph.
 */
public class TransportPPLQueryActionCancellationTest {

  private final QueryJobService jobs = mock(QueryJobService.class);
  private final PPLService pplService = mock(PPLService.class);
  private final NodeClient client = mock(NodeClient.class);
  private final SecurityAdapter security = mock(SecurityAdapter.class);
  private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

  @SuppressWarnings("unchecked")
  private final ActionListener<TransportPPLQueryResponse> listener = mock(ActionListener.class);

  private TransportPPLQueryAction action;
  private Method submitAsync;

  @Before
  public void setUp() throws Exception {
    action = mock(TransportPPLQueryAction.class, CALLS_REAL_METHODS);
    setField("clientRef", client);
    setField("queryJobService", jobs);
    setField("securityAdapter", security);
    ThreadPool pool = mock(ThreadPool.class);
    when(client.threadPool()).thenReturn(pool);
    when(pool.getThreadContext()).thenReturn(threadContext);
    submitAsync =
        TransportPPLQueryAction.class.getDeclaredMethod(
            "submitAsync",
            Task.class,
            PPLService.class,
            PPLQueryRequest.class,
            ActionListener.class,
            Consumer.class);
    submitAsync.setAccessible(true);
  }

  @Test
  public void unsupportedTaskIsRejectedBeforePublishingOrExecutingAJob() {
    InvocationTargetException failure =
        assertThrows(InvocationTargetException.class, () -> submit(mock(Task.class)));

    assertTrue(failure.getCause() instanceof IllegalArgumentException);
    assertEquals(
        "Async PPL queries require a cancellable PPLQueryTask", failure.getCause().getMessage());
    verifyNoInteractions(jobs, pplService, client, security, listener);
  }

  @Test
  public void submittedRunnerCancelsTheOriginalPplTask() throws Exception {
    PPLQueryTask task =
        new TransportPPLQueryRequest("source=test", null, "/_plugins/_ppl")
            .createTask(1, "transport", PPLQueryAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
    when(security.current()).thenReturn(Principal.UNSECURED);
    when(jobs.submit(any(), eq(Principal.UNSECURED), any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new QueryResult.Running(new QueryJobId("owner-node", "query-context"))));

    submit(task);

    ArgumentCaptor<QueryRunner> runner = ArgumentCaptor.forClass(QueryRunner.class);
    verify(jobs).submit(runner.capture(), eq(Principal.UNSECURED), any(), any());
    verify(listener).onResponse(any());
    runner.getValue().cancel();
    assertTrue(task.isCancelled());
    assertEquals("async PPL query cancelled", task.getReasonCancelled());
    verifyNoInteractions(pplService);
  }

  private void submit(Task task) throws Exception {
    PPLQueryRequest request =
        new PPLQueryRequest("source=test", new JSONObject(), "/_plugins/_ppl", "jdbc");
    submitAsync.invoke(
        action, task, pplService, request, listener, PPLService.NO_ANONYMIZED_QUERY_SINK);
  }

  private void setField(String name, Object value) throws Exception {
    Field field = TransportPPLQueryAction.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(action, value);
  }
}
