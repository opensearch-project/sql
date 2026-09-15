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
  public void testSupportsResourceTracking() {
    // Resource tracking must be enabled so the coordinator task's CPU/memory is exposed through
    // the tasks API for Query Insights.
    assertTrue(newTask().supportsResourceTracking());
  }

  @Test
  public void testQueryInsightsParentHeaderName() {
    // Query Insights relies on this exact header name to classify child DSL searches and associate
    // them with the originating query; it must match what SQLPlugin.getTaskHeaders() registers.
    assertEquals("X-Query-Insights-Parent", QueryInsightsMarker.PARENT_HEADER);
  }

  @Test
  public void testQueryInsightsParentHeaderValueIsSourcePrefixed() {
    // Value format is <source>:<nodeId>:<taskId> so QI reads both source and parent id from one
    // header. SQL will reuse the same helper with source "SQL".
    assertEquals("PPL:node-1:42", QueryInsightsMarker.value("PPL", "node-1", 42L));
  }
}
