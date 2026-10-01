/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Duration;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;

/**
 * Query job lifecycle service.
 *
 * <p>The service does not compile queries, execute them, or store results. It wraps a caller-built
 * {@link QueryRunner} in a {@link QueryJob}, publishes the job to a {@link QueryJobStore}, and
 * authorizes later observers against the job owner. Each engine (PPL, SQL, analytics-engine)
 * constructs the runner it needs directly and hands it to {@link #submit(QueryRunner, Principal,
 * Duration)}; no language dispatch happens inside the service.
 */
public interface QueryJobService {

  /**
   * Wraps the given runner in a {@link QueryJob}, publishes the job, starts the runner, arms
   * retention for {@code keepAlive}, and returns the job. Callers observe progress through {@link
   * QueryJob#await(Duration)}, {@link QueryJob#onTerminal(Runnable)}, or {@link QueryJob#status()}.
   *
   * @param keepAlive retention TTL applied after the job reaches a terminal state; must be non-null
   *     and positive
   */
  QueryJob submit(QueryRunner runner, Principal submitter, Duration keepAlive);

  /**
   * Removes {@code job} from the store if it is still registered. Idempotent and safe to invoke
   * when the retention timer has already evicted it (or when no retention was ever armed). Used by
   * callers that render the terminal response inline and therefore know no polling GET will ever
   * reach for the job — pinning it in the store for the full {@code keep_alive} would otherwise
   * leak the result rows on the heap.
   */
  void discard(QueryJob job);

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
