/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;

/**
 * Query job lifecycle service.
 *
 * <p>Each engine (PPL, SQL, analytics-engine) supplies its own {@link QueryRunner}. The service
 * publishes a {@link QueryJob}, owns the submission wait and retention decision, and authorizes
 * later observers against the job owner. No language dispatch happens inside the service.
 */
public interface QueryJobService {

  /**
   * Starts the runner and races its completion against {@code waitForCompletion}. Returns the
   * terminal result inline when the runner wins, or {@link QueryResult.Running} when the wait
   * expires. Inline success and failure remove the job before the returned stage completes.
   * Retention is attached only when a {@code Running} result exposes an id for polling.
   *
   * @param runner engine adapter that will produce the result
   * @param submitter caller identity retained for authorization
   * @param waitForCompletion submission wait; must be non-null and non-negative
   * @param keepAlive retention TTL applied after the job reaches a terminal state; must be non-null
   *     and positive
   * @return the submission outcome, exceptionally completed with the runner's cause on inline
   *     failure or cancellation
   */
  CompletionStage<QueryResult> submit(
      QueryRunner runner, Principal submitter, Duration waitForCompletion, Duration keepAlive);

  /**
   * Returns a snapshot for the given job.
   *
   * @throws QueryJobNotFoundException when the ID does not resolve on this node
   * @throws QueryJobForbiddenException when the caller is not the owner
   */
  QueryJobStatus get(QueryJobId id, Principal caller);

  /**
   * Cancels the job and returns the resulting snapshot. Idempotent; cancelling an already-terminal
   * job returns its existing status.
   *
   * @throws QueryJobNotFoundException when the ID does not resolve on this node
   * @throws QueryJobForbiddenException when the caller is not the owner
   */
  QueryJobStatus cancel(QueryJobId id, Principal caller);
}
