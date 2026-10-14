/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.junit.Test;
import org.opensearch.core.tasks.TaskId;

public class SqlQueryTaskTest {

  private SqlQueryTask newTask() {
    return new SqlQueryTask(
        1, "transport", SqlQueryAction.NAME, "SELECT 1", TaskId.EMPTY_TASK_ID, Map.of());
  }

  @Test
  public void testShouldCancelChildrenReturnsTrue() {
    assertTrue(newTask().shouldCancelChildrenOnCancellation());
  }

  @Test
  public void testCancellation() {
    SqlQueryTask task = newTask();
    assertFalse(task.isCancelled());
    task.cancel("Test");
    assertTrue(task.isCancelled());
  }
}
