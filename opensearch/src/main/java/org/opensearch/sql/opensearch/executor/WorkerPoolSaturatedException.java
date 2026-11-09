/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.Locale;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

/**
 * A SQL worker pool refused a query because its queue is full. The client may retry the query
 * unchanged. The message names only the pool, never the node or the task.
 */
public class WorkerPoolSaturatedException extends OpenSearchRejectedExecutionException {

  /**
   * @param poolName the pool that refused the query
   */
  public WorkerPoolSaturatedException(String poolName) {
    super(String.format(Locale.ROOT, "[%s] thread pool is saturated; retry later", poolName));
  }
}
