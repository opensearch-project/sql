/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;

/**
 * OpenSearch-hosted {@link QueryJobService} implementation.
 *
 * <p>Symmetric with the existing {@code OpenSearchQueryManager}: the neutral interface lives in
 * {@code core}, this class supplies the OpenSearch-side wiring (node-id, clock, in-memory store).
 * Kept in the {@code org.opensearch.sql.job} package so it can construct {@link QueryJob} and
 * invoke its package-private {@code startRunner()} without widening the visibility of either.
 *
 * <p>Job-id collisions are handled by retrying with a fresh {@link QueryJobId}. Because {@link
 * QueryJobId#create(String)} draws from {@link java.util.UUID#randomUUID}, collisions are not
 * observed in practice.
 */
public final class OpenSearchQueryJobService implements QueryJobService {

  private final QueryJobStore store;
  private final ClusterService clusterService;
  private final Clock clock;
  private final RetentionPolicy retentionPolicy;
  private final Function<Throwable, Map<String, Object>> failureRenderer;

  /**
   * Legacy constructor without a failure renderer; existing tests and the no-wiring case keep
   * working. Equivalent to passing {@code null} for {@code failureRenderer}.
   */
  public OpenSearchQueryJobService(
      QueryJobStore store,
      ClusterService clusterService,
      Clock clock,
      RetentionPolicy retentionPolicy) {
    this(store, clusterService, clock, retentionPolicy, null);
  }

  /**
   * @param store registry that will hold submitted jobs
   * @param clusterService source of the local node id used to mint routable {@link QueryJobId}s;
   *     resolved lazily on every submit so identity changes across restarts are observed
   * @param clock time source for the state machine
   * @param retentionPolicy attaches eviction only to jobs exposed for polling; may be {@code null}
   *     to disable retention (test-only)
   * @param failureRenderer supplies the structured {@code details} payload captured on {@link
   *     QueryFailure} when a job transitions to {@code FAILED}; may be {@code null} (empty
   *     details). Forwarded to every submitted {@link QueryJob} and invoked at most once per job.
   * @throws NullPointerException if {@code store}, {@code clusterService}, or {@code clock} is
   *     {@code null}
   */
  public OpenSearchQueryJobService(
      QueryJobStore store,
      ClusterService clusterService,
      Clock clock,
      RetentionPolicy retentionPolicy,
      Function<Throwable, Map<String, Object>> failureRenderer) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.retentionPolicy = retentionPolicy;
    this.failureRenderer = failureRenderer;
  }

  @Override
  public CompletionStage<QueryResult> submit(
      QueryRunner runner, Principal submitter, Duration waitForCompletion, Duration keepAlive) {
    Objects.requireNonNull(runner, "runner must not be null");
    Objects.requireNonNull(submitter, "submitter must not be null");
    Objects.requireNonNull(waitForCompletion, "waitForCompletion must not be null");
    Objects.requireNonNull(keepAlive, "keepAlive must not be null");
    if (waitForCompletion.isNegative()) {
      throw new IllegalArgumentException("waitForCompletion must not be negative");
    }
    if (!keepAlive.isPositive()) {
      throw new IllegalArgumentException("keepAlive must be positive");
    }
    QueryJob job = publish(runner, submitter);
    job.startRunner();
    return job.await(waitForCompletion)
        .whenComplete(
            (result, error) -> {
              if (error == null && result instanceof QueryResult.Running) {
                if (retentionPolicy != null) {
                  retentionPolicy.arm(job, keepAlive);
                }
              } else {
                store.remove(job.id(), job);
              }
            });
  }

  @Override
  public QueryJobStatus get(QueryJobId id, Principal caller) {
    QueryJob job = requireJob(id);
    authorize(job, caller);
    return job.status();
  }

  @Override
  public QueryJobStatus cancel(QueryJobId id, Principal caller) {
    QueryJob job = requireJob(id);
    authorize(job, caller);
    job.cancel();
    return job.status();
  }

  /**
   * Mints a fresh {@link QueryJobId}, constructs the {@link QueryJob}, and inserts it into the
   * store. Retries on the (in practice unreachable) UUID collision so the store's invariant "one
   * job per id" holds without callers seeing partial state.
   *
   * @param runner engine adapter that will produce the result
   * @param owner caller identity retained with the job for later authorization
   * @return the registered job, ready to be started
   */
  private QueryJob publish(QueryRunner runner, Principal owner) {
    while (true) {
      QueryJobId id = QueryJobId.create(clusterService.localNode().getId());
      QueryJob job = new QueryJob(id, owner, runner, clock, failureRenderer);
      if (store.register(job) == null) {
        return job;
      }
    }
  }

  /**
   * Resolves {@code id} against the store or raises {@link QueryJobNotFoundException}. Extracted
   * from {@link #get} and {@link #cancel} so both surface the same "not-found" error uniformly.
   *
   * @param id id supplied by the caller; must be non-{@code null}
   * @return the job registered under {@code id}
   * @throws QueryJobNotFoundException when the id does not resolve on this node
   */
  private QueryJob requireJob(QueryJobId id) {
    Objects.requireNonNull(id, "id must not be null");
    return store.find(id).orElseThrow(() -> new QueryJobNotFoundException(id));
  }

  /**
   * Denies access when {@code caller} is not allowed to observe or mutate {@code job}. The
   * exception message intentionally omits the owner's identity — see {@link
   * QueryJobForbiddenException}.
   *
   * @param job job the caller is trying to reach
   * @param caller identity resolved from the current request
   * @throws QueryJobForbiddenException when {@code caller} is not the owner
   */
  private static void authorize(QueryJob job, Principal caller) {
    Objects.requireNonNull(caller, "caller must not be null");
    if (!job.owner().allows(caller)) {
      throw new QueryJobForbiddenException();
    }
  }
}
