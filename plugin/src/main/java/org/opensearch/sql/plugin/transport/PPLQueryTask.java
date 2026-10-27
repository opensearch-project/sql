/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.opensearch.executor.ThreadResourceAccounting;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskResourceTrackingService;

public class PPLQueryTask extends CancellableTask implements ThreadResourceAccounting {

  // The fields below are captured on the request thread and read later by the report listener
  // (which runs on a different thread), for the Query Insights record.

  /** Security {@code _opendistro_security_user_info} string; null when unsecured. */
  private volatile String queryInsightsUserInfo;

  /** The query text as submitted; null until the query has parsed. */
  private volatile String queryInsightsQueryText;

  /** Whether the query failed; recorded so a failed PPL query is flagged in Top N. */
  private volatile boolean queryInsightsFailed = false;

  /** Milliseconds from the task's start to its response; -1 until the response is sent. */
  private volatile long queryInsightsLatencyMillis = -1L;

  /** Whether the statement is an {@code explain}, which plans but runs no search. */
  private volatile boolean queryInsightsExplain = false;

  /**
   * Whether an outer query's parent marker was already in the thread context, so this query's child
   * searches are attributed to that query rather than to this one. No in-process caller does this
   * today; the flag keeps the record honest if one is ever added.
   */
  private volatile boolean queryInsightsNested = false;

  /**
   * The {@code <source>:<nodeId>:<taskId>} marker stamped on this query's child searches; null when
   * it was never stamped, so the record does not advertise a marker no child carries.
   */
  private volatile String queryInsightsParentMarker;

  /**
   * Core's tracking service, which the engine's thread scopes report to; null (scopes are no-ops)
   * on every route but execute: explain, analyze, async submission and the analytics engine.
   */
  private volatile TaskResourceTrackingService resourceTracking;

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

  public void setQueryInsightsQueryText(String queryText) {
    this.queryInsightsQueryText = queryText;
  }

  public String getQueryInsightsQueryText() {
    return queryInsightsQueryText;
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

  public void setQueryInsightsParentMarker(String parentMarker) {
    this.queryInsightsParentMarker = parentMarker;
  }

  public String getQueryInsightsParentMarker() {
    return queryInsightsParentMarker;
  }

  public void setQueryInsightsFailed(boolean failed) {
    this.queryInsightsFailed = failed;
  }

  public boolean isQueryInsightsFailed() {
    return queryInsightsFailed;
  }

  public void setQueryInsightsLatencyMillis(long latencyMillis) {
    this.queryInsightsLatencyMillis = latencyMillis;
  }

  public long getQueryInsightsLatencyMillis() {
    return queryInsightsLatencyMillis;
  }

  /**
   * Has the engine's thread scopes report to {@code service}. Set on the execute routes, where the
   * engine runs the query; other routes leave the scopes as no-ops.
   */
  public void setResourceTracking(TaskResourceTrackingService service) {
    this.resourceTracking = service;
  }

  /**
   * CPU time core tracked on this task: the engine's threads, plus any work core's task-aware pools
   * ran for it. Child searches are tracked on their own tasks, so they are not included.
   */
  public long getCpuNanos() {
    return getTotalResourceStats().getCpuTimeInNanos();
  }

  /** Bytes allocated by the threads core tracked on this task; see {@link #getCpuNanos()}. */
  public long getAllocatedBytes() {
    return getTotalResourceStats().getMemoryInBytes();
  }

  /**
   * Tracked like a search task, so core's {@code startTracking} registers it when {@code
   * task_resource_tracking.enabled} is on and the JVM can measure threads. Otherwise the tracking
   * service ignores the engine's scopes, so the cluster setting decides whether anything is
   * recorded.
   */
  @Override
  public boolean supportsResourceTracking() {
    return true;
  }

  /**
   * Tracks the current thread against this task until the scope closes, through core's tracking
   * service: the usage lands in the task's {@code resource_stats}, so {@code _tasks} and Live
   * Queries show it while the query runs (list tasks refreshes threads still working). A tracked
   * thread also holds the task's resource-tracking count, so the completion listener the Query
   * Insights report hangs off fires only after every scope has recorded its usage.
   */
  @Override
  public Scope enterThread() {
    final TaskResourceTrackingService service = resourceTracking;
    if (service == null) {
      return Scope.NOOP;
    }
    final Thread thread = Thread.currentThread();
    final long threadId = thread.threadId();
    // Core rejects a second start on a thread it is already tracking for this task.
    if (!accountedThreads.add(threadId)) {
      return Scope.NOOP;
    }
    service.taskExecutionStartedOnThread(getId(), threadId);
    return () -> {
      assert Thread.currentThread() == thread : "scope closed on a different thread";
      try {
        service.taskExecutionFinishedOnThread(getId(), threadId);
      } finally {
        accountedThreads.remove(threadId);
      }
    };
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }
}
