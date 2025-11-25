/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/** QueryManager implemented in OpenSearch cluster. */
@RequiredArgsConstructor
public class OpenSearchQueryManager implements QueryManager {

  private static final Logger LOG = LogManager.getLogger(OpenSearchQueryManager.class);

  private final NodeClient nodeClient;

  private final Settings settings;

  private static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";

  @Override
  public QueryId submit(AbstractPlan queryPlan) {
    TimeValue timeout = settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT);
    schedule(nodeClient, queryPlan::execute, timeout);

    return queryPlan.getQueryId();
  }

  private void schedule(NodeClient client, Runnable task, TimeValue timeout) {
    ThreadPool threadPool = client.threadPool();

    // Wrap the task to track completion and handle timeout
    Runnable wrappedTask =
        withCurrentContext(
            () -> {
              // Create a flag to track if the task has completed
              final Thread executionThread = Thread.currentThread();

              // Schedule a timeout task
              Scheduler.ScheduledCancellable timeoutTask =
                  threadPool.schedule(
                      () -> {
                        LOG.warn(
                            "Query execution timed out after {}. Interrupting execution thread.",
                            timeout);
                        executionThread.interrupt();
                      },
                      timeout,
                      SQL_WORKER_THREAD_POOL_NAME);

              try {
                task.run();
                // If task completes successfully, cancel the timeout
                timeoutTask.cancel();
              } catch (Exception e) {
                // Cancel timeout on any exception
                timeoutTask.cancel();

                // Check if this was a timeout-related interruption
                if (Thread.interrupted() || e.getCause() instanceof InterruptedException) {
                  LOG.error("Query was interrupted due to timeout after {}", timeout);
                  throw new RuntimeException(
                      new TimeoutException("Query execution timed out after " + timeout));
                }
                // Re-throw the original exception if not timeout-related
                throw e;
              }
            });

    threadPool.schedule(wrappedTask, new TimeValue(0), SQL_WORKER_THREAD_POOL_NAME);
  }

  private Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}
