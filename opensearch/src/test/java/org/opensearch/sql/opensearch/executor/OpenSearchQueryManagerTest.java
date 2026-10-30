/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlan;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchQueryManagerTest {

  @Mock private QueryId queryId;

  @Mock private QueryService queryService;

  @Mock private QueryType queryType;

  @Mock private UnresolvedPlan plan;

  @Mock private ResponseListener<ExecutionEngine.QueryResponse> listener;

  @Test
  public void submitQuery() {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    Settings settings = mock(Settings.class);
    Scheduler.ScheduledCancellable mockScheduledTask = mock(Scheduler.ScheduledCancellable.class);

    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(TimeValue.timeValueSeconds(60));

    AtomicBoolean isRun = new AtomicBoolean(false);
    AbstractPlan queryPlan =
        new QueryPlan(queryId, queryType, plan, queryService, listener) {
          @Override
          public void execute() {
            isRun.set(true);
          }
        };

    // Run the worker task immediately; the timeout timer it arms is a mock ScheduledCancellable.
    ExecutorService worker = mock(ExecutorService.class);
    when(threadPool.executor(OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME))
        .thenReturn(worker);
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
            })
        .when(worker)
        .execute(any());
    doAnswer(invocation -> mockScheduledTask).when(threadPool).schedule(any(), any(), any());
    new OpenSearchQueryManager(nodeClient, settings).submit(queryPlan);

    assertTrue(isRun.get());
  }

  @Test
  public void submitQueryRejectedBySaturatedPoolThrowsWorkerPoolSaturated() {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    Settings settings = mock(Settings.class);
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(TimeValue.timeValueSeconds(60));
    ExecutorService worker = mock(ExecutorService.class);
    when(threadPool.executor(OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME))
        .thenReturn(worker);
    doThrow(
            new OpenSearchRejectedExecutionException(
                "rejected execution of task on OpenSearchThreadPoolExecutor[name ="
                    + " node-1/sql-worker]"))
        .when(worker)
        .execute(any());

    AtomicBoolean isRun = new AtomicBoolean(false);
    AbstractPlan queryPlan =
        new QueryPlan(queryId, queryType, plan, queryService, listener) {
          @Override
          public void execute() {
            isRun.set(true);
          }
        };

    WorkerPoolSaturatedException e =
        assertThrows(
            WorkerPoolSaturatedException.class,
            () -> new OpenSearchQueryManager(nodeClient, settings).submit(queryPlan));
    assertEquals("[sql-worker] thread pool is saturated; retry later", e.getMessage());
    assertFalse(isRun.get());
  }
}
