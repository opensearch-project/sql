/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
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
  public static final String SQL_COMPLEX_WORKER_THREAD_POOL_NAME = "sql-complex-worker";
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

  /**
   * Set when this query's deadline expires, for a query that has no task to carry the signal. Boxed
   * because the scheduled timeout writes it from another thread.
   */
  private static final ThreadLocal<AtomicBoolean> deadlineFlag = new ThreadLocal<>();

  static void setDeadlineFlag(AtomicBoolean flag) {
    deadlineFlag.set(flag);
  }

  static void clearDeadlineFlag() {
    deadlineFlag.remove();
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
              final AtomicBoolean deadlineExpired = new AtomicBoolean(false);

              Scheduler.ScheduledCancellable timeoutTask =
                  threadPool.schedule(
                      () -> {
                        LOG.warn(
                            "Query execution timed out after {}. Stopping the query.", timeout);
                        deadlineExpired.set(true);
                        expire(cancelTask, executionThread, timeout);
                      },
                      timeout,
                      ThreadPool.Names.GENERIC);

              setCancellableTask(cancelTask);
              setDeadlineFlag(deadlineExpired);

              try {
                task.run();
                timeoutTask.cancel();
                // Clear any leftover thread interrupts to keep the thread pool clean
                Thread.interrupted();
              } catch (Exception e) {
                timeoutTask.cancel();
                Thread.interrupted();

                if (timedOut(cancelTask, deadlineExpired)) {
                  LOG.error("Query was stopped after exceeding the timeout of {}", timeout);
                  throw new QueryTimeoutException(timeout);
                }

                throw e;
              } finally {
                clearCancellableTask();
                clearDeadlineFlag();
              }
            });

    try {
      // The executor runs the task in this thread's ThreadContext.
      threadPool.executor(SQL_WORKER_THREAD_POOL_NAME).execute(wrappedTask);
    } catch (OpenSearchRejectedExecutionException e) {
      throw new WorkerPoolSaturatedException(SQL_WORKER_THREAD_POOL_NAME);
    }
  }

  private static final String TIMEOUT_REASON_PREFIX = "Query execution timed out after ";

  /**
   * Whether this query was stopped by its own deadline. The task is shared across the worker and
   * complex-worker threads and carries the reason {@link #expire} cancelled it, so it answers for
   * whichever thread observes the failure. {@code localFlag} covers a query submitted with no task.
   */
  static boolean timedOut(@Nullable CancellableTask task, @Nullable AtomicBoolean localFlag) {
    if (localFlag != null && localFlag.get()) {
      return true;
    }
    AtomicBoolean carried = deadlineFlag.get();
    if (carried != null && carried.get()) {
      return true;
    }
    return task != null
        && task.isCancelled()
        && task.getReasonCancelled() != null
        && task.getReasonCancelled().startsWith(TIMEOUT_REASON_PREFIX);
  }

  /**
   * Stops a query whose deadline expired. Cancelling reaches work that is not blocked on I/O, since
   * {@code OpenSearchIndexEnumerator#moveNext} tests {@link CancellableTask#isCancelled()} per row;
   * the interrupt releases a thread blocked on a fetch.
   */
  static void expire(
      @Nullable CancellableTask cancelTask, Thread executionThread, TimeValue timeout) {
    if (cancelTask != null && !cancelTask.isCancelled()) {
      cancelTask.cancel(TIMEOUT_REASON_PREFIX + timeout);
    }
    executionThread.interrupt();
  }

  private Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}
