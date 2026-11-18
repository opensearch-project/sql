/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.sql.ppl.QueryInsightsMetadata;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Records PPL queries in Query Insights Top N. {@link TransportPPLQueryAction} picks the route a
 * query takes; on the route that records, this class does the rest. It reads the setting that turns
 * recording on, keeps on the task what the record reports, stamps the parent marker the query's
 * searches carry, and sends the record once the task is done.
 */
final class PPLQueryInsightsCoordinator {

  private static final Logger LOG = LogManager.getLogger(PPLQueryInsightsCoordinator.class);

  /**
   * Toggle for recording PPL queries into Query Insights. Registered by the Query Insights plugin
   * ({@code QueryInsightsSettings.TOP_N_PPL_QUERIES_ENABLED}, default false).
   */
  static final String RECORDING_ENABLED_KEY = "search.insights.top_queries.ppl.enabled";

  /** Security plugin's authenticated-user transient ({@code name|backendroles|roles|...}). */
  private static final String SECURITY_USER_INFO_TRANSIENT = "_opendistro_security_user_info";

  private final ClusterService clusterService;
  private final TransportService transportService;
  private final ThreadPool threadPool;

  PPLQueryInsightsCoordinator(
      ClusterService clusterService, TransportService transportService, ThreadPool threadPool) {
    this.clusterService = clusterService;
    this.transportService = transportService;
    this.threadPool = threadPool;
  }

  /**
   * Whether the query running as {@code task} is recorded: it is this plugin's PPL task, Query
   * Insights is installed, and the toggle is on. Disabled on any failure.
   */
  boolean isEnabledFor(PPLQueryTask task) {
    if (task == null) {
      return false;
    }
    try {
      Setting<?> setting = clusterService.getClusterSettings().get(RECORDING_ENABLED_KEY);
      if (setting == null) {
        return false;
      }
      Object value = clusterService.getClusterSettings().get(setting);
      return Boolean.TRUE.equals(value);
    } catch (Exception e) {
      LOG.debug("Failed to evaluate Query Insights PPL recording gate; defaulting to disabled", e);
      return false;
    }
  }

  /**
   * Keeps on {@code task} what its record says about the parsed query: the query as written,
   * literals included, as Query Insights does for a DSL search's source, and whether it is an
   * {@code explain}. Does nothing without a task.
   */
  Consumer<QueryInsightsMetadata> sink(PPLQueryTask task, String queryText) {
    if (task == null) {
      return metadata -> {};
    }
    return metadata -> {
      task.setQueryInsightsQueryText(queryText);
      task.setQueryInsightsExplain(metadata.explain());
    };
  }

  /**
   * Records on the task how the query ended and its latency as the response goes out, for the Query
   * Insights record written later. Returns {@code delegate} unchanged when there is no PPL task.
   */
  ActionListener<TransportPPLQueryResponse> recordCompletion(
      ActionListener<TransportPPLQueryResponse> delegate, PPLQueryTask task) {
    if (task == null) {
      return delegate;
    }
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse response) {
        task.setQueryInsightsLatencyMillis(elapsedMillis(task));
        delegate.onResponse(response);
      }

      @Override
      public void onFailure(Exception e) {
        task.setQueryInsightsLatencyMillis(elapsedMillis(task));
        task.setQueryInsightsFailed(true);
        delegate.onFailure(e);
      }
    };
  }

  /**
   * Runs a query that is recorded. {@code execution} starts the query and answers through the
   * listener it is handed.
   *
   * <p>That listener runs in the caller's context, captured before stamping, so the callback runs
   * without the marker: the worker thread that completes the query carries it, and an in-process
   * caller invoked via NodeClient would otherwise inherit it in onResponse.
   *
   * <p>The marker is stamped inside a stored context, so it is dropped from this thread when this
   * returns, keeping a caller that issues several PPL queries from tagging later ones (or a plain
   * DSL search) with this query's marker. The query's searches still see it: the executor captures
   * the thread context when the work is scheduled, which happens inside {@code execution}.
   */
  void runRecorded(
      PPLQueryTask task,
      ActionListener<TransportPPLQueryResponse> listener,
      Consumer<ActionListener<TransportPPLQueryResponse>> execution) {
    final ThreadContext threadContext = threadPool.getThreadContext();
    ActionListener<TransportPPLQueryResponse> callerListener =
        ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
    try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(true)) {
      stampParentMarker(task);
      registerReport(task);
      execution.accept(callerListener);
    }
  }

  /**
   * Stamp the parent marker so child DSL searches carry it back to this PPL query. The marker is
   * stored on the task only once it is actually in the thread context, and the record later sends
   * whatever was stored: if stamping fails the record goes out with no marker rather than one that
   * no child search carries.
   */
  private void stampParentMarker(PPLQueryTask task) {
    try {
      ThreadContext threadContext = threadPool.getThreadContext();
      String value =
          QueryInsightsMarker.value("PPL", clusterService.localNode().getId(), task.getId());
      // An outer marker is kept rather than replaced: putHeader rejects a duplicate key, and
      // clearing the context first would drop the authenticated-user headers the child searches
      // need. The child searches then belong to that outer query, so this one does not report a
      // record -- see shouldReport.
      if (threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER) == null) {
        threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, value);
        task.setQueryInsightsParentMarker(value);
      } else {
        task.setQueryInsightsNested(true);
      }
      // Capture the user here; the report listener runs on a thread without the transient.
      task.setQueryInsightsUserInfo(threadContext.getTransient(SECURITY_USER_INFO_TRANSIENT));
    } catch (Exception e) {
      LOG.warn("Failed to stamp Query Insights parent header for query association", e);
    }
  }

  /**
   * Report the finished PPL query once the task is unregistered and every accounting scope has
   * closed, so the measured usage is final.
   */
  private void registerReport(PPLQueryTask task) {
    try {
      boolean registered =
          task.addResourceTrackingCompletionListener(
              new NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(Task completed) {
                  report((PPLQueryTask) completed);
                }

                @Override
                protected void innerOnFailure(Exception e) {}
              });
      if (registered == false) {
        LOG.debug(
            "Query Insights report listener not registered; resource tracking already complete");
      }
    } catch (Exception e) {
      LOG.warn("Failed to register Query Insights report listener", e);
    }
  }

  /**
   * Whether a finished task should produce a Top N record. Decided from what the metadata sink
   * captured rather than from the route, because the route cannot see either case: {@code explain}
   * is valid inside the query text (not just on {@code /_explain}), and a query that fails to parse
   * never reaches the sink at all, so there is no parsed query to report.
   *
   * <p>A nested query is also skipped: its child searches carry the outer query's marker, so a
   * record built from this task's own id would advertise a marker no child references, and the
   * children's cost would be counted against both records.
   */
  static boolean shouldReport(PPLQueryTask task) {
    return task.getQueryInsightsQueryText() != null
        && !task.isQueryInsightsExplain()
        && !task.isQueryInsightsNested();
  }

  /** Serialize a completed PPL query and send it to Query Insights; errors are ignored. */
  private void report(PPLQueryTask task) {
    if (!shouldReport(task)) {
      return;
    }
    try {
      String nodeId = clusterService.localNode().getId();
      String queryText = task.getQueryInsightsQueryText();

      long cpuNanos = task.getCpuNanos();
      long memoryBytes = task.getAllocatedBytes();

      // Taken when the response was sent (see recordCompletion), not now: this runs once the last
      // thread scope closes, which a lookahead search still running on the background pool can
      // hold open well past the response.
      long latencyMillis = task.getQueryInsightsLatencyMillis();
      if (latencyMillis < 0) {
        latencyMillis = elapsedMillis(task);
      }

      // Start time, not report time: Query Insights stamps a search record with the request's
      // start (SearchRequest#getOrCreateAbsoluteStartMillis). Stamping completion here would put a
      // query that crosses a Top N window boundary in a later window than its own child searches,
      // and the marker roll-up would then miss them.
      long timestampMillis = task.getStartTime();

      // The marker stamped on child DSL tasks, so their cpu/memory rolls up into this parent; null
      // when stamping failed, which the reporter sends as an empty marker.
      String parentMarker = task.getQueryInsightsParentMarker();

      String userInfo = task.getQueryInsightsUserInfo();
      boolean failed = task.isQueryInsightsFailed();

      QueryInsightsReporter.report(
          transportService,
          clusterService.localNode(),
          "PPL",
          parentMarker,
          nodeId,
          queryText,
          timestampMillis,
          latencyMillis,
          cpuNanos,
          memoryBytes,
          userInfo,
          failed);
    } catch (Exception e) {
      LOG.debug("Failed to write PPL query to Query Insights", e);
    }
  }

  /**
   * Wall-clock (monotonic) time since the task started. The profile total is not used: it collapses
   * to ~0 across the thread that finishes the query.
   */
  private static long elapsedMillis(PPLQueryTask task) {
    return Math.max(0L, (System.nanoTime() - task.getStartTimeNanos()) / 1_000_000L);
  }
}
