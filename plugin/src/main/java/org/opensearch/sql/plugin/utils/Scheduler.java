/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.utils;

import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

/** The scheduler which schedule the task run in sql-worker thread pool. */
@UtilityClass
public class Scheduler {

  public static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";

  public static void schedule(NodeClient client, Runnable task) {
    ThreadPool threadPool = client.threadPool();
    threadPool.schedule(withCurrentContext(task), new TimeValue(0), SQL_WORKER_THREAD_POOL_NAME);
  }

  private static Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}