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
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
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

  public static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";
  public static final String SQL_BACKGROUND_THREAD_POOL_NAME = "sql_background_io";

  public interface CancellationCallBack {
        void onExecutionThreadAvailable(Thread thread);
        void onExecutionComplete();
        boolean isCancelled();
  }

  public static ThreadLocal<CancellationCallBack> cancellationCallBackThreadLocal = new ThreadLocal<>();

  public static void setCancellationCallback(CancellationCallBack value) {
      cancellationCallBackThreadLocal.set(value);
  }

  public static void clearCancellationCallback() {
      cancellationCallBackThreadLocal.remove();
  }

  @Override
  public QueryId submit(AbstractPlan queryPlan) {
    TimeValue timeout = settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT);
    CancellationCallBack callBack = cancellationCallBackThreadLocal.get();
    cancellationCallBackThreadLocal.remove();
    schedule(nodeClient, queryPlan::execute, timeout, callBack);

    return queryPlan.getQueryId();
  }

  private void schedule(NodeClient client, Runnable task, TimeValue timeout, CancellationCallBack callBack) {
    ThreadPool threadPool = client.threadPool();

    Runnable wrappedTask =
        withCurrentContext(
            () -> {
              final Thread executionThread = Thread.currentThread();

              if (callBack != null) {
                  callBack.onExecutionThreadAvailable(executionThread);
              }

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

              try {
                task.run();
                timeoutTask.cancel();
                // Clear any leftover thread interrupts to keep the thread pool clean
                Thread.interrupted();
              } catch (Exception e) {
                timeoutTask.cancel();

                // Special-case handling of timeout-related interruptions
                if (Thread.interrupted() || e.getCause() instanceof InterruptedException) {
                    if (callBack != null && callBack.isCancelled()) {
                        LOG.info("Query was cancelled");
                        throw new OpenSearchException("Query was cancelled.");
                    }
                  LOG.error("Query was interrupted due to timeout after {}", timeout);
                  throw new OpenSearchTimeoutException(
                      "Query execution timed out after " + timeout);
                }

                throw e;
              }
              finally {
                  if (callBack != null) {
                      callBack.onExecutionComplete();
                  }
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
