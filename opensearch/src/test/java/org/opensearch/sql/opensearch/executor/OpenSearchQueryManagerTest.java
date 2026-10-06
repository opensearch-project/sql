/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
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

    // Mock the schedule method to run tasks immediately and return a mock ScheduledCancellable
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return mockScheduledTask;
            })
        .when(threadPool)
        .schedule(any(), any(), any());
    new OpenSearchQueryManager(nodeClient, settings).submit(queryPlan);

    assertTrue(isRun.get());
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
    assertEquals("scope must close after execution", 0, task.openScopes.get());
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

    doAnswer(
            invocation -> {
              if ("sql-worker".equals(invocation.getArgument(2))) {
                invocation.<Runnable>getArgument(0).run();
              }
              return mockScheduledTask;
            })
        .when(threadPool)
        .schedule(any(), any(), any());

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
