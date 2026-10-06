/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.*;

import java.util.Map;
import org.junit.Test;
import org.opensearch.core.tasks.TaskId;

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
  public void testSupportsResourceTrackingOffByDefault() {
    // Off by default so a PPL query adds no resource-tracking overhead unless Query Insights
    // recording (and core's task resource tracking) are enabled.
    assertFalse(newTask().supportsResourceTracking());
  }

  @Test
  public void testSupportsResourceTrackingWhenEnabled() {
    PPLQueryTask task = newTask();
    task.setResourceTrackingEnabled(true);
    assertTrue(task.supportsResourceTracking());
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
