/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.common.response.ResponseListener;

/**
 * Opt-in listener for Calcite executions that can publish intermediate query snapshots.
 *
 * <p>The normal {@link ResponseListener} contract remains unchanged. Execution engines should call
 * {@link #onPartial(ExecutionEngine.QueryResponse)} only when the optimized physical plan is known
 * to produce semantically valid preview rows. {@link UpdateMode#APPEND} previews are immutable
 * prefixes; {@link UpdateMode#REPLACE} previews are complete snapshots that replace the previous
 * snapshot.
 */
public interface ProgressiveQueryResponseListener
    extends ResponseListener<ExecutionEngine.QueryResponse> {

  /** Whether rows already returned by a running query are immutable. */
  enum UpdateMode {
    APPEND,
    REPLACE
  }

  /**
   * Query-level execution progress.
   *
   * <p>The value is an approximate, finite, monotonic indication of query progress. A running query
   * never reports {@code 1.0}; only successful completion does.
   */
  record QueryProgress(double fractionDone) {
    public static final QueryProgress ZERO = new QueryProgress(0D);

    public QueryProgress {
      if (!Double.isFinite(fractionDone) || fractionDone < 0D || fractionDone > 1D) {
        throw new IllegalArgumentException("fractionDone must be finite and between 0 and 1");
      }
    }
  }

  /** Called once the logical and physical plans establish the fixed update mode for the job. */
  default void onQueryClassified(UpdateMode updateMode) {}

  /** Publishes progress without changing the current result rows. */
  default void onProgress(QueryProgress progress) {}

  /**
   * Registers a currently running OpenSearch search task with the job.
   *
   * <p>The callback is intentionally a {@link Runnable} so the core execution contract does not
   * depend on OpenSearch server task classes.
   */
  default void onSearchTaskStarted(long operationId, Runnable cancelAction) {}

  /** Removes a completed OpenSearch search task from the job cancellation set. */
  default void onSearchTaskFinished(long operationId) {}

  /** Publishes the current rows according to the job's fixed {@link UpdateMode}. */
  void onPartial(ExecutionEngine.QueryResponse response);

  /**
   * Atomically publishes rows and the progress represented by those rows.
   *
   * <p>Implementations that persist asynchronous job snapshots should override this method so rows
   * and progress are committed atomically. The default preserves compatibility for other listeners.
   */
  default void onPartial(ExecutionEngine.QueryResponse response, QueryProgress progress) {
    onProgress(progress);
    onPartial(response);
  }
}
