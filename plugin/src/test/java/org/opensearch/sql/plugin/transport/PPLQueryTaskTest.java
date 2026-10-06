/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.opensearch.executor.ThreadResourceAccounting;
import org.opensearch.tasks.Task;

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

  @Test
  public void testAccountingOffByDefault() {
    PPLQueryTask task = newTask();
    assertSame(ThreadResourceAccounting.Scope.NOOP, task.enterThread());
    // Never touches core's tracking, so _tasks resource_stats stays empty.
    assertFalse(task.supportsResourceTracking());
    assertTrue(task.getResourceStats().isEmpty());
  }

  @Test
  public void testAccountingRecordsUsageWithoutTouchingResourceStats() {
    PPLQueryTask task = newTask();
    task.setResourceAccountingEnabled(true);
    try (ThreadResourceAccounting.Scope ignored = task.enterThread()) {
      burnCpu();
    }
    assertTrue(task.getCpuNanos() > 0);
    assertTrue(task.getAllocatedBytes() > 0);
    assertFalse(task.supportsResourceTracking());
    assertTrue(task.getResourceStats().isEmpty());
  }

  @Test
  public void testNestedScopeOnSameThreadIsNotCountedTwice() {
    PPLQueryTask task = newTask();
    task.setResourceAccountingEnabled(true);
    try (ThreadResourceAccounting.Scope outer = task.enterThread()) {
      assertSame(ThreadResourceAccounting.Scope.NOOP, task.enterThread());
    }
  }

  @Test
  public void testReportWaitsForOpenScopes() {
    PPLQueryTask task = newTask();
    task.setResourceAccountingEnabled(true);
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
    task.setQueryInsightsAnonymizedQuery("explain source=table | fields identifier");
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
    task.setQueryInsightsAnonymizedQuery("source=table | where identifier > ***");
    task.setQueryInsightsFailed(true);
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
