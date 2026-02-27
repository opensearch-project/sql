/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.junit.Test;
import org.opensearch.core.tasks.TaskId;

import java.util.Map;

import static org.junit.Assert.*;

public class SQLQueryTaskTest {

    @Test
    public void testOnCancelledInterruptsThread() {
        SQLQueryTask sqlQueryTask = new SQLQueryTask(1, "transport", "cluster:admin/opensearch/ppl", "test query", TaskId.EMPTY_TASK_ID, Map.of());
        sqlQueryTask.setExecutionThread(Thread.currentThread());
        sqlQueryTask.cancel("testing");
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }

    @Test
    public void testOnCancelledWithNoThread() {
        SQLQueryTask sqlQueryTask = new SQLQueryTask(1, "transport", "cluster:admin/opensearch/ppl", "test query", TaskId.EMPTY_TASK_ID, Map.of());
        sqlQueryTask.cancel("testing");
    }

    @Test
    public void testClearExecutionThread() {
        SQLQueryTask sqlQueryTask = new SQLQueryTask(1, "transport", "cluster:admin/opensearch/ppl", "test query", TaskId.EMPTY_TASK_ID, Map.of());
        sqlQueryTask.setExecutionThread(Thread.currentThread());
        sqlQueryTask.clearExecutionThread();
        sqlQueryTask.cancel("testing");
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void testShouldCancelChildrenReturnsFalse() {
        SQLQueryTask sqlQueryTask = new SQLQueryTask(1, "transport", "cluster:admin/opensearch/ppl", "test query", TaskId.EMPTY_TASK_ID, Map.of());
        assertFalse(sqlQueryTask.shouldCancelChildrenOnCancellation());
    }

    @Test
    public void testCreateTaskReturnsSQLQueryTask() {
        TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
        SQLQueryTask task = transportPPLQueryRequest.createTask(1, "transport", "cluster:admin/opensearch/ppl", TaskId.EMPTY_TASK_ID, Map.of());
        assertNotNull(task);
    }

    @Test
    public void testWithQueryId () {
        TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
        transportPPLQueryRequest.queryId("test-123");
        assertEquals("PPL [queryId=test-123]: source=t a=1", transportPPLQueryRequest.getDescription());
    }

    @Test
    public void testWithoutQueryId () {
        TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest("source=t a=1", null, "/_plugins/_ppl");
        assertEquals("PPL: source=t a=1", transportPPLQueryRequest.getDescription());
    }
}
