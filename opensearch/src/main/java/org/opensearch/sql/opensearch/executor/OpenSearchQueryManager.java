/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.apache.logging.log4j.ThreadContext.getImmutableContext;
import static org.apache.logging.log4j.ThreadContext.putAll;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
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

  /** Samples per-thread CPU/memory for resource tracking; null when the JVM bean is unavailable. */
  private static final ThreadMXBean THREAD_MX_BEAN = resolveThreadMXBean();

  private static ThreadMXBean resolveThreadMXBean() {
    try {
      if (ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean) {
        return bean;
      }
    } catch (Exception e) {
      LOG.warn("Per-thread resource metrics unavailable; PPL task resource tracking disabled", e);
    }
    return null;
  }

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

  private static final String QUERY_INSIGHTS_PARENT_HEADER = "X-Query-Insights-Parent";

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

              // Bracket resource tracking for the inline path (runs on this thread). Script plans
              // hand off to the complex-worker pool, which brackets itself.
              final boolean trackResources =
                  cancelTask != null && cancelTask.supportsResourceTracking();
              final long trackedThreadId = Thread.currentThread().getId();
              final boolean trackingStarted =
                  trackResources && startThreadResourceTracking(cancelTask, trackedThreadId);

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
                if (trackingStarted) {
                  stopThreadResourceTracking(cancelTask, trackedThreadId);
                }
                clearCancellableTask();
              }
            });

    threadPool.schedule(wrappedTask, new TimeValue(0), SQL_WORKER_THREAD_POOL_NAME);
  }

  private Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = getImmutableContext();
    // Carry the parent-marker header across the pool hop; the hop doesn't preserve it otherwise.
    final ThreadContext osThreadContext = nodeClient.threadPool().getThreadContext();
    final String parentMarker = osThreadContext.getHeader(QUERY_INSIGHTS_PARENT_HEADER);
    return () -> {
      putAll(currentContext);
      if (parentMarker != null && osThreadContext.getHeader(QUERY_INSIGHTS_PARENT_HEADER) == null) {
        osThreadContext.putHeader(QUERY_INSIGHTS_PARENT_HEADER, parentMarker);
      }
      task.run();
    };
  }

  /**
   * Records the starting CPU/memory snapshot for {@code threadId}.
   *
   * @return true if tracking started; only then should {@link #stopThreadResourceTracking} be
   *     called.
   */
  static boolean startThreadResourceTracking(CancellableTask task, long threadId) {
    try {
      task.startThreadResourceTracking(
          threadId, ResourceStatsType.WORKER_STATS, currentThreadResourceMetrics(threadId));
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to start resource tracking for task [{}]", task.getId(), e);
      return false;
    }
  }

  /** Records the final CPU/memory snapshot for {@code threadId}. */
  static void stopThreadResourceTracking(CancellableTask task, long threadId) {
    try {
      task.stopThreadResourceTracking(
          threadId, ResourceStatsType.WORKER_STATS, currentThreadResourceMetrics(threadId));
    } catch (Exception e) {
      LOG.warn("Failed to stop resource tracking for task [{}]", task.getId(), e);
    }
  }

  /** Per-thread memory and CPU usage; empty array when the JVM bean is unavailable. */
  private static ResourceUsageMetric[] currentThreadResourceMetrics(long threadId) {
    if (THREAD_MX_BEAN == null) {
      return new ResourceUsageMetric[0];
    }
    ResourceUsageMetric memory =
        new ResourceUsageMetric(
            ResourceStats.MEMORY, THREAD_MX_BEAN.getThreadAllocatedBytes(threadId));
    ResourceUsageMetric cpu =
        new ResourceUsageMetric(ResourceStats.CPU, THREAD_MX_BEAN.getThreadCpuTime(threadId));
    return new ResourceUsageMetric[] {memory, cpu};
  }
}
