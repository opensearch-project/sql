/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

/**
 * Coordinator task for a SQL query. Registering this task in the {@code TaskManager} gives every
 * DSL search task spawned by the SQL engine a parent reference back to the originating SQL query,
 * so downstream consumers (e.g. query-insights) can correlate the DSL searches with their SQL
 * source and look up the original request from the task description.
 */
public class SqlQueryTask extends CancellableTask {
  public SqlQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers) {
    super(id, type, action, description, parentTaskId, headers);
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }
}
