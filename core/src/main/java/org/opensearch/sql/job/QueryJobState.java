/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

/**
 * Public lifecycle state of a query job.
 *
 * <p>The state model deliberately excludes retention and caller-waiting concerns. The
 * BigQuery-style invariant is that a job stays {@link #RUNNING} regardless of whether a client is
 * still holding a response socket open. Whether callers block on the result is a transport concern;
 * whether the job is retained after completion is a store concern. Neither belongs on this enum.
 */
public enum QueryJobState {
  /** The job has been submitted but its runner has not started producing a result. */
  PENDING,

  /** The runner is producing the result. */
  RUNNING,

  /** The runner completed successfully. */
  SUCCEEDED,

  /** The runner completed with an error visible to the caller. */
  FAILED,

  /** The job was cancelled through {@link QueryJob#cancel()} before completion. */
  CANCELLED;

  /** Returns {@code true} for the three terminal states. */
  public boolean isTerminal() {
    return this == SUCCEEDED || this == FAILED || this == CANCELLED;
  }
}
