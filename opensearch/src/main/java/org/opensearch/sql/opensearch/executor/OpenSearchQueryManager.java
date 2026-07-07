/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/** QueryManager implemented in OpenSearch cluster. */
@RequiredArgsConstructor
public class OpenSearchQueryManager implements QueryManager {

  private static final Logger LOG = LogManager.getLogger(OpenSearchQueryManager.class);

  private final NodeClient nodeClient;

  private final Settings settings;

  public static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";
  public static final String SQL_BACKGROUND_THREAD_POOL_NAME = "sql_background_io";

  private static final ThreadLocal<CancellableTask> cancellableTask = new ThreadLocal<>();

  public static void setCancellableTask(CancellableTask task) {
    cancellableTask.set(task);
  }

  public static CancellableTask getCancellableTask() {
    return cancellableTask.get();
  }

  public static void clearCancellableTask() {
    cancellableTask.remove();
  }

  @Override
  public QueryId submit(AbstractPlan queryPlan) {
    TimeValue timeout = settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT);
    CancellableTask cancelTask = cancellableTask.get();
    cancellableTask.remove();
    schedule(nodeClient, queryPlan::execute, timeout, cancelTask);

    return queryPlan.getQueryId();
  }

  private void schedule(
      NodeClient client, Runnable task, TimeValue timeout, CancellableTask cancelTask) {
    ThreadPool threadPool = client.threadPool();

    Runnable wrappedTask =
        withCurrentContext(
            () -> {
              final Thread executionThread = Thread.currentThread();

              Scheduler.ScheduledCancellable timeoutTask =
                  threadPool.schedule(
                      () -> {
                        LOG.warn(
                            "Query execution timed out after {}. Interrupting execution thread.",
                            timeout);
                        executionThread.interrupt();
                      },
                      timeout,
                      ThreadPool.Names.GENERIC);

              setCancellableTask(cancelTask);

              try {
                task.run();
                timeoutTask.cancel();
                // Clear any leftover thread interrupts to keep the thread pool clean
                Thread.interrupted();
              } catch (Exception e) {
                timeoutTask.cancel();

                // Special-case handling of timeout-related interruptions
                if (Thread.interrupted() || e.getCause() instanceof InterruptedException) {
                  LOG.error("Query was interrupted due to timeout after {}", timeout);
                  throw new OpenSearchTimeoutException(
                      "Query execution timed out after " + timeout);
                }

                throw e;
              } finally {
                clearCancellableTask();
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
