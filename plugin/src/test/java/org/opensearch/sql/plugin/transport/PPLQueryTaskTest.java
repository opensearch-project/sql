/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.opensearch.executor.ThreadResourceAccounting;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ThreadPool;

public class PPLQueryTaskTest {

  @Test
  public void testShouldCancelChildrenReturnsTrue() {
    PPLQueryTask pplQueryTask =
        new PPLQueryTask(
            1,
            "transport",
            "cluster:admin/opensearch/ppl",
            "test query",
            TaskId.EMPTY_TASK_ID,
            Map.of());
    assertTrue(pplQueryTask.shouldCancelChildrenOnCancellation());
  }

  @Test
  public void testCreateTaskReturnsPPLQueryTask() {
    TransportPPLQueryRequest transportPPLQueryRequest =
        new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
    PPLQueryTask task =
        transportPPLQueryRequest.createTask(
            1, "transport", "cluster:admin/opensearch/ppl", TaskId.EMPTY_TASK_ID, Map.of());
    assertNotNull(task);
  }

  @Test
  public void testWithQueryId() {
    TransportPPLQueryRequest transportPPLQueryRequest =
        new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
    transportPPLQueryRequest.queryId("test-123");
    assertEquals("PPL [queryId=test-123]: source=t a=1", transportPPLQueryRequest.getDescription());
  }

  @Test
  public void testWithoutQueryId() {
    TransportPPLQueryRequest transportPPLQueryRequest =
        new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
    assertEquals("PPL: source=t a=1", transportPPLQueryRequest.getDescription());
  }

  @Test
  public void testCooperativeModel() {
    TransportPPLQueryRequest transportPPLQueryRequest =
        new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
    PPLQueryTask task =
        transportPPLQueryRequest.createTask(
            1, "transport", "cluster:admin/opensearch/ppl", TaskId.EMPTY_TASK_ID, Map.of());
    assertFalse(task.isCancelled());
    task.cancel("Test");
    assertTrue(task.isCancelled());
  }

  private PPLQueryTask newTask() {
    return new PPLQueryTask(
        1,
        "transport",
        "cluster:admin/opensearch/ppl",
        "test query",
        TaskId.EMPTY_TASK_ID,
        Map.of());
  }

  /** Core's tracking service as a node builds it, with {@code task_resource_tracking.enabled}. */
  private static TaskResourceTrackingService trackingService(boolean enabled) {
    Settings settings =
        Settings.builder()
            .put(TaskResourceTrackingService.TASK_RESOURCE_TRACKING_ENABLED.getKey(), enabled)
            .build();
    ThreadPool threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    return new TaskResourceTrackingService(
        settings,
        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
        threadPool);
  }

  /** A task as the execute route leaves it: registered by core, scopes wired to the service. */
  private PPLQueryTask trackedTask(TaskResourceTrackingService service) {
    PPLQueryTask task = newTask();
    // What TransportAction.execute does before the action runs; a no-op when tracking is off.
    service.startTracking(task).close();
    task.setResourceTracking(service);
    return task;
  }

  @Test
  public void testTrackedLikeASearchTask() {
    assertTrue(newTask().supportsResourceTracking());
  }

  @Test
  public void testScopesAreNoOpsOnRoutesThatRunNoQuery() {
    PPLQueryTask task = newTask();
    assertSame(ThreadResourceAccounting.Scope.NOOP, task.enterThread());
    assertTrue(task.getResourceStats().isEmpty());
  }

  @Test
  public void testScopeRecordsThreadUsageInResourceStats() {
    PPLQueryTask task = trackedTask(trackingService(true));
    try (ThreadResourceAccounting.Scope ignored = task.enterThread()) {
      burnCpu();
    }
    // The task's resource_stats, which _tasks and Live Queries read; the record reads the same.
    assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
    assertTrue(task.getTotalResourceStats().getMemoryInBytes() > 0);
    assertEquals(task.getTotalResourceStats().getCpuTimeInNanos(), task.getCpuNanos());
    assertEquals(task.getTotalResourceStats().getMemoryInBytes(), task.getAllocatedBytes());
  }

  @Test
  public void testOpenScopeShowsOnRefresh() {
    // List tasks refreshes threads still working, so a query's usage shows before its scope ends.
    TaskResourceTrackingService service = trackingService(true);
    PPLQueryTask task = trackedTask(service);
    try (ThreadResourceAccounting.Scope ignored = task.enterThread()) {
      burnCpu();
      service.refreshResourceStats(task);
      assertTrue(task.getTotalResourceStats().getCpuTimeInNanos() > 0);
    }
  }

  @Test
  public void testNothingRecordedWhenTaskResourceTrackingIsOff() {
    PPLQueryTask task = trackedTask(trackingService(false));
    try (ThreadResourceAccounting.Scope ignored = task.enterThread()) {
      burnCpu();
    }
    assertTrue(task.getResourceStats().isEmpty());
    assertEquals(0L, task.getCpuNanos());
  }

  @Test
  public void testNestedScopeOnSameThreadIsNotTrackedTwice() {
    PPLQueryTask task = trackedTask(trackingService(true));
    try (ThreadResourceAccounting.Scope outer = task.enterThread()) {
      // Core rejects a second start on a thread it already tracks for the task.
      assertSame(ThreadResourceAccounting.Scope.NOOP, task.enterThread());
    }
    assertEquals(1, task.getResourceStats().get(Thread.currentThread().threadId()).size());
  }

  @Test
  public void testReportWaitsForOpenScopes() {
    PPLQueryTask task = trackedTask(trackingService(true));
    AtomicInteger fired = new AtomicInteger();
    assertTrue(
        task.addResourceTrackingCompletionListener(
            new NotifyOnceListener<>() {
              @Override
              protected void innerOnResponse(Task t) {
                fired.incrementAndGet();
              }

              @Override
              protected void innerOnFailure(Exception e) {}
            }));

    ThreadResourceAccounting.Scope scope = task.enterThread();
    // What TaskManager.unregister does once the response is sent.
    task.decrementResourceTrackingThreads();
    assertEquals("must not report while a thread is still accounting", 0, fired.get());

    scope.close();
    assertEquals(1, fired.get());
  }

  private static void burnCpu() {
    long x = 0;
    for (int i = 0; i < 1_000_000; i++) {
      x += Integer.toString(i).hashCode();
    }
    assertNotEquals(42, x);
  }

  @Test
  public void testQueryInsightsFailedOffByDefault() {
    assertFalse(newTask().isQueryInsightsFailed());
  }

  @Test
  public void testQueryInsightsFailedFlagIsSettable() {
    PPLQueryTask task = newTask();
    task.setQueryInsightsFailed(true);
    assertTrue(task.isQueryInsightsFailed());
  }

  @Test
  public void testInlineExplainIsNotReported() {
    // "explain source=t | ..." reaches the execute route because isExplainRequest() only looks at
    // the path, so the sink's flag is what keeps it out of Top N.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("explain source=table | fields identifier");
    task.setQueryInsightsExplain(true);
    assertFalse(TransportPPLQueryAction.shouldReportToQueryInsights(task));
  }

  @Test
  public void testSyntaxErrorIsNotReported() {
    // A parse failure throws before the sink runs, leaving no query text to report.
    assertFalse(TransportPPLQueryAction.shouldReportToQueryInsights(newTask()));
  }

  @Test
  public void testRuntimeFailureAfterPlanningIsReported() {
    // Planning succeeded, so the metadata is present; a later execution failure must still report.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("source=table | where identifier > 42");
    task.setQueryInsightsFailed(true);
    assertTrue(TransportPPLQueryAction.shouldReportToQueryInsights(task));
  }

  @Test
  public void testNestedQueryIsNotReported() {
    // An outer marker was already in the context, so the child searches rolled up into that query.
    // Reporting here would advertise a marker no child references and double count their cost.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("source=table | where identifier > 42");
    task.setQueryInsightsNested(true);
    assertFalse(TransportPPLQueryAction.shouldReportToQueryInsights(task));
  }

  @Test
  public void testParentMarkerIsAbsentUntilStamped() {
    // The record sends whatever marker was stamped. A task whose stamp never happened must carry
    // none, so the record does not advertise a marker no child search references.
    PPLQueryTask task = newTask();
    assertNull(task.getQueryInsightsParentMarker());
    task.setQueryInsightsParentMarker("PPL:node-1:42");
    assertEquals("PPL:node-1:42", task.getQueryInsightsParentMarker());
  }

  @Test
  public void testNonNestedQueryIsReportedByDefault() {
    // Guards the default: a plain query must not be mistaken for a nested one.
    PPLQueryTask task = newTask();
    task.setQueryInsightsQueryText("source=table | where identifier > 42");
    assertFalse("nested must default to false", task.isQueryInsightsNested());
    assertTrue(TransportPPLQueryAction.shouldReportToQueryInsights(task));
  }

  @Test
  public void testQueryInsightsParentHeaderName() {
    assertEquals("X-Query-Insights-Parent", QueryInsightsMarker.PARENT_HEADER);
  }

  @Test
  public void testQueryInsightsParentHeaderValueIsSourcePrefixed() {
    // Value format is <source>:<nodeId>:<taskId> so QI reads both source and parent id from one
    // header. SQL will reuse the same helper with source "SQL".
    assertEquals("PPL:node-1:42", QueryInsightsMarker.value("PPL", "node-1", 42L));
  }
}
