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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.ThreadResourceAccounting;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlan;
import org.opensearch.tasks.CancellableTask;
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

  @Test
  public void expiredDeadlineSurfacesTimeoutException() {
    QueryTimeoutException e =
        assertThrows(
            QueryTimeoutException.class,
            () ->
                submitAndFailWith(
                    new RuntimeException(new SQLException("The task is cancelled.")), true));
    assertEquals("Query execution timed out after 1m", e.getMessage());
    assertEquals(RestStatus.REQUEST_TIMEOUT, e.status());
  }

  @Test
  public void failureWithoutAnExpiredDeadlinePropagatesUnchanged() {
    IllegalStateException original = new IllegalStateException("something else broke");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> submitAndFailWith(original, false));
    assertEquals(original, e);
  }

  @Test
  public void expireCancelsTheTaskAndInterruptsTheThread() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);

    OpenSearchQueryManager.expire(task, Thread.currentThread(), TimeValue.timeValueSeconds(5));

    assertTrue(Thread.interrupted());
    verify(task).cancel("Query execution timed out after 5s");
  }

  /**
   * The dispatcher's synchronous branch has no flag of its own, and a query submitted without a
   * task has nothing to cancel, so the thread local is the only signal left on that path.
   */
  @Test
  public void aTasklessQueryIsRecognisedFromTheThreadLocal() {
    AtomicBoolean deadline = new AtomicBoolean(false);
    OpenSearchQueryManager.setDeadlineFlag(deadline);
    try {
      assertFalse(OpenSearchQueryManager.timedOut(null, null));

      deadline.set(true);

      assertTrue(OpenSearchQueryManager.timedOut(null, null));
    } finally {
      OpenSearchQueryManager.clearDeadlineFlag();
    }
  }

  @Test
  public void aTasklessQueryWithNoSignalIsNotATimeout() {
    assertFalse(OpenSearchQueryManager.timedOut(null, null));
  }

  /** A task cancelled for some other reason is not this query's deadline. */
  @Test
  public void aTaskCancelledForAnotherReasonIsNotATimeout() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(true);
    when(task.getReasonCancelled()).thenReturn("async PPL query cancelled");

    assertFalse(OpenSearchQueryManager.timedOut(task, null));
  }

  @Test
  public void expireToleratesAQueryWithNoTask() {
    OpenSearchQueryManager.expire(null, Thread.currentThread(), TimeValue.timeValueSeconds(5));
    assertTrue(Thread.interrupted());
  }

  /**
   * Submits a plan whose execution throws {@code failure}, optionally firing the scheduled timeout
   * first so the deadline has expired by the time the failure is handled.
   */
  private void submitAndFailWith(RuntimeException failure, boolean expireDeadline) {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    Settings settings = mock(Settings.class);
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(TimeValue.timeValueSeconds(60));

    AtomicReference<Runnable> onTimeout = new AtomicReference<>();
    doAnswer(
            invocation -> {
              onTimeout.set(invocation.getArgument(0));
              return mock(Scheduler.ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(), any(), any());

    AbstractPlan queryPlan =
        new QueryPlan(queryId, queryType, plan, queryService, listener) {
          @Override
          public void execute() {
            if (expireDeadline) {
              onTimeout.get().run();
            }
            throw failure;
          }
        };

    ExecutorService worker = mock(ExecutorService.class);
    when(threadPool.executor(OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME))
        .thenReturn(worker);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(worker)
        .execute(any());

    new OpenSearchQueryManager(nodeClient, settings).submit(queryPlan);
  }

  @AfterEach
  public void clearTask() {
    OpenSearchQueryManager.clearCancellableTask();
  }

  @Test
  public void accountingScopeSpansExecution() {
    AccountingTask task = new AccountingTask();
    OpenSearchQueryManager.setCancellableTask(task);

    runSubmit(() -> task.openDuringExecute.set(task.openScopes.get() == 1), null);

    assertTrue("scope must be open while the plan executes", task.openDuringExecute.get());
    assertEquals(1, task.entered.get());
    assertEquals(0, task.openScopes.get(), "scope must close after execution");
  }

  @Test
  public void accountingScopeClosesWhenExecutionThrows() {
    AccountingTask task = new AccountingTask();
    OpenSearchQueryManager.setCancellableTask(task);
    RuntimeException boom = new RuntimeException("execute failed");

    assertThrows(RuntimeException.class, () -> runSubmit(null, boom));

    assertEquals(1, task.entered.get());
    assertEquals(0, task.openScopes.get());
  }

  @Test
  public void propagatesExecutionException() {
    RuntimeException boom = new RuntimeException("execute failed");
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> runSubmit(null, boom));
    assertEquals(boom, thrown);
  }

  /** Runs a query through the manager, executing only the worker task inline. */
  private void runSubmit(Runnable onExecute, RuntimeException toThrow) {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    Settings settings = mock(Settings.class);
    Scheduler.ScheduledCancellable mockScheduledTask = mock(Scheduler.ScheduledCancellable.class);

    when(nodeClient.threadPool()).thenReturn(threadPool);

    when(settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(TimeValue.timeValueSeconds(60));

    AbstractPlan queryPlan =
        new QueryPlan(queryId, queryType, plan, queryService, listener) {
          @Override
          public void execute() {
            if (onExecute != null) {
              onExecute.run();
            }
            if (toThrow != null) {
              throw toThrow;
            }
          }
        };

    // Run the worker task inline; the timeout timer it arms is a mock ScheduledCancellable.
    ExecutorService worker = mock(ExecutorService.class);
    when(threadPool.executor(OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME))
        .thenReturn(worker);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(worker)
        .execute(any());
    doAnswer(invocation -> mockScheduledTask).when(threadPool).schedule(any(), any(), any());

    new OpenSearchQueryManager(nodeClient, settings).submit(queryPlan);
  }

  /** CancellableTask that counts the accounting scopes the engine opens on it. */
  private static class AccountingTask extends CancellableTask implements ThreadResourceAccounting {
    final AtomicInteger entered = new AtomicInteger();
    final AtomicInteger openScopes = new AtomicInteger();
    final AtomicBoolean openDuringExecute = new AtomicBoolean();

    AccountingTask() {
      super(1L, "ppl", "action", "desc", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    @Override
    public Scope enterThread() {
      entered.incrementAndGet();
      openScopes.incrementAndGet();
      return openScopes::decrementAndGet;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
      return true;
    }
  }
}
