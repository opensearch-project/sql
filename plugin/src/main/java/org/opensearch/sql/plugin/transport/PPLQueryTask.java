/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.opensearch.executor.ThreadResourceAccounting;
import org.opensearch.tasks.CancellableTask;

public class PPLQueryTask extends CancellableTask implements ThreadResourceAccounting {

  /** Null when the JVM can't report per-thread CPU and allocation; accounting is then a no-op. */
  private static final ThreadMXBean THREAD_MX_BEAN = resolveThreadMXBean();

  // The fields below are captured on the request thread and read later by the report listener
  // (which runs on a different thread), for the Query Insights record.

  /** Security {@code _opendistro_security_user_info} string; null when unsecured. */
  private volatile String queryInsightsUserInfo;

  /** Source index name(s) resolved from the AST; empty when unresolved. */
  private volatile List<String> queryInsightsIndices = List.of();

  /** Anonymized query text ({@code PPLQueryDataAnonymizer}); null when anonymization didn't run. */
  private volatile String queryInsightsAnonymizedQuery;

  /** Whether the query failed; recorded so a failed PPL query is flagged in Top N. */
  private volatile boolean queryInsightsFailed = false;

  /** Whether the statement is an {@code explain}, which plans but runs no search. */
  private volatile boolean queryInsightsExplain = false;

  /**
   * Whether an outer query's parent marker was already in the thread context, so this query's child
   * searches are attributed to that query rather than to this one. No in-process caller does this
   * today; the flag keeps the record honest if one is ever added.
   */
  private volatile boolean queryInsightsNested = false;

  /** Whether this query's thread usage is measured; off unless Query Insights will report it. */
  private volatile boolean resourceAccountingEnabled = false;

  private final LongAdder cpuNanos = new LongAdder();
  private final LongAdder allocatedBytes = new LongAdder();

  /** Threads with an open scope, so a nested scope on the same thread isn't counted twice. */
  private final Set<Long> accountedThreads = ConcurrentHashMap.newKeySet();

  public PPLQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers) {
    super(id, type, action, description, parentTaskId, headers);
  }

  public void setQueryInsightsUserInfo(String userInfo) {
    this.queryInsightsUserInfo = userInfo;
  }

  public String getQueryInsightsUserInfo() {
    return queryInsightsUserInfo;
  }

  public void setQueryInsightsIndices(List<String> indices) {
    this.queryInsightsIndices = indices == null ? List.of() : indices;
  }

  public List<String> getQueryInsightsIndices() {
    return queryInsightsIndices;
  }

  public void setQueryInsightsAnonymizedQuery(String anonymizedQuery) {
    this.queryInsightsAnonymizedQuery = anonymizedQuery;
  }

  public String getQueryInsightsAnonymizedQuery() {
    return queryInsightsAnonymizedQuery;
  }

  public void setQueryInsightsExplain(boolean explain) {
    this.queryInsightsExplain = explain;
  }

  public boolean isQueryInsightsExplain() {
    return queryInsightsExplain;
  }

  public void setQueryInsightsNested(boolean nested) {
    this.queryInsightsNested = nested;
  }

  public boolean isQueryInsightsNested() {
    return queryInsightsNested;
  }

  public void setQueryInsightsFailed(boolean failed) {
    this.queryInsightsFailed = failed;
  }

  public boolean isQueryInsightsFailed() {
    return queryInsightsFailed;
  }

  public void setResourceAccountingEnabled(boolean enabled) {
    this.resourceAccountingEnabled = enabled && THREAD_MX_BEAN != null;
  }

  /** CPU time used on the SQL plugin's threads; child searches are measured by core. */
  public long getCpuNanos() {
    return cpuNanos.sum();
  }

  /** Bytes allocated on the SQL plugin's threads; child searches are measured by core. */
  public long getAllocatedBytes() {
    return allocatedBytes.sum();
  }

  /**
   * Measures the current thread until the scope closes. The scope also holds the task's
   * resource-tracking thread count, so the completion listener the Query Insights report hangs off
   * fires only after every scope has recorded its usage. Nothing is written to the task's {@code
   * resource_stats}, so {@code _tasks} output and {@code task_resource_tracking.enabled} are
   * untouched.
   */
  @Override
  public Scope enterThread() {
    if (!resourceAccountingEnabled) {
      return Scope.NOOP;
    }
    final Thread thread = Thread.currentThread();
    final long threadId = thread.threadId();
    if (!accountedThreads.add(threadId)) {
      return Scope.NOOP;
    }
    incrementResourceTrackingThreads();
    final long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
    final long allocatedStart = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes();
    return () -> {
      assert Thread.currentThread() == thread : "scope closed on a different thread";
      try {
        addDelta(cpuNanos, cpuStart, THREAD_MX_BEAN.getCurrentThreadCpuTime());
        addDelta(allocatedBytes, allocatedStart, THREAD_MX_BEAN.getCurrentThreadAllocatedBytes());
      } finally {
        accountedThreads.remove(threadId);
        decrementResourceTrackingThreads();
      }
    };
  }

  /** The bean returns -1 when the measurement is unsupported or disabled; record nothing then. */
  private static void addDelta(LongAdder total, long start, long end) {
    if (start >= 0 && end >= start) {
      total.add(end - start);
    }
  }

  private static ThreadMXBean resolveThreadMXBean() {
    try {
      if (ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean
          && bean.isCurrentThreadCpuTimeSupported()
          && bean.isThreadAllocatedMemorySupported()) {
        return bean;
      }
    } catch (Exception e) {
      // Fall through: accounting stays off.
    }
    return null;
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }
}
