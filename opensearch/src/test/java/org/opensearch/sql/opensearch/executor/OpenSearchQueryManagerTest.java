/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ThreadResourceInfo;
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
  public void tracksResourceUsageWhenTaskSupportsIt() {
    TrackingTask task = new TrackingTask(true);
    OpenSearchQueryManager.setCancellableTask(task);

    runSubmit();

    // The worker thread should have been bracketed with start/stop tracking, leaving a completed
    // (inactive) resource entry on the coordinator task.
    Map<Long, List<ThreadResourceInfo>> stats = task.getResourceStats();
    assertEquals(1, stats.size());
    List<ThreadResourceInfo> infos = stats.values().iterator().next();
    assertEquals(1, infos.size());
    assertFalse("resource entry should be closed after execution", infos.get(0).isActive());
  }

  @Test
  public void skipsResourceTrackingWhenTaskDoesNotSupportIt() {
    TrackingTask task = new TrackingTask(false);
    OpenSearchQueryManager.setCancellableTask(task);

    runSubmit();

    assertTrue("no resource entries expected", task.getResourceStats().isEmpty());
  }

  /** Runs a trivial query through the manager with the schedule hook executing tasks inline. */
  private void runSubmit() {
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
          public void execute() {}
        };

    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return mockScheduledTask;
            })
        .when(threadPool)
        .schedule(any(), any(), any());

    new OpenSearchQueryManager(nodeClient, settings).submit(queryPlan);
  }

  /** Minimal CancellableTask whose resource-tracking support is configurable. */
  private static class TrackingTask extends CancellableTask {
    private final boolean supportsTracking;

    TrackingTask(boolean supportsTracking) {
      super(1L, "ppl", "action", "desc", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
      this.supportsTracking = supportsTracking;
    }

    @Override
    public boolean supportsResourceTracking() {
      return supportsTracking;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
      return true;
    }
  }
}
