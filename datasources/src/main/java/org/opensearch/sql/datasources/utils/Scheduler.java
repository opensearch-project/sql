/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.utils;

import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;

/** The scheduler which schedule the task run in sql-worker thread pool. */
@UtilityClass
public class Scheduler {
  public static void schedule(NodeClient client, Runnable task) {
    schedule(client, task, SQL_WORKER_THREAD_POOL_NAME);
  }

  /**
   * Schedule {@code task} on the named thread pool while preserving the caller's thread context.
   * Lets short, cheap operations run off {@code sql-worker} so they don't contend with async query
   * execution on a saturated pool.
   */
  public static void schedule(NodeClient client, Runnable task, String poolName) {
    ThreadPool threadPool = client.threadPool();
    threadPool.schedule(withCurrentContext(task), new TimeValue(0), poolName);
  }

  private static Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}
