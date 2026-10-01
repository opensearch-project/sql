/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Evicts terminal jobs from a {@link QueryJobStore} after a caller-supplied TTL.
 *
 * <p>Retention lives at the service layer, not on {@link QueryJob}. The state machine has no
 * retention axis; retention is orthogonal to state. Terminal jobs are kept just long enough for a
 * client to observe them, then dropped so the store's memory footprint stays bounded.
 */
public final class RetentionPolicy {

  private static final Logger LOG = LogManager.getLogger(RetentionPolicy.class);
  private static final String GENERIC_POOL = ThreadPool.Names.GENERIC;

  private final QueryJobStore store;
  private final ThreadPool threadPool;
  // Tracks the pending eviction task per job so callers can cancel it when they discard the
  // result inline. Without this, the scheduler's queue retains a strong reference to the job
  // for the full keep_alive — the store entry is gone but the heap retention lives on.
  private final ConcurrentMap<QueryJobId, Scheduler.ScheduledCancellable> pending =
      new ConcurrentHashMap<>();

  public RetentionPolicy(QueryJobStore store, ThreadPool threadPool) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
  }

  /**
   * Attaches an eviction timer to {@code job}'s completion. When the job reaches a terminal state,
   * a delayed task on the generic pool removes it from the store after {@code ttl}. Idempotent
   * against the store: removal is conditional on the same job still being registered.
   *
   * @param job the newly-submitted job; must already be registered in the store
   * @param ttl retention window; must be non-null and positive
   */
  public void arm(QueryJob job, Duration ttl) {
    Objects.requireNonNull(job, "job must not be null");
    Objects.requireNonNull(ttl, "ttl must not be null");
    if (!ttl.isPositive()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
    job.onTerminal(() -> scheduleEviction(job, ttl));
  }

  /**
   * Cancels any pending eviction task for {@code jobId}. Idempotent — safe to call when no task is
   * armed, when the task has already fired, or when the same id is disarmed twice. The dropped
   * reference lets the scheduler reclaim the captured {@link QueryJob} immediately rather than
   * holding it until the TTL elapses.
   */
  public void disarm(QueryJobId jobId) {
    Objects.requireNonNull(jobId, "jobId must not be null");
    Scheduler.ScheduledCancellable cancellable = pending.remove(jobId);
    if (cancellable != null) {
      cancellable.cancel();
    }
  }

  private void scheduleEviction(QueryJob job, Duration ttl) {
    try {
      Scheduler.ScheduledCancellable cancellable =
          threadPool.schedule(
              () -> {
                // Drop the tracking entry first so the task cannot keep itself alive via the
                // ConcurrentMap after it fires.
                pending.remove(job.id());
                boolean removed = store.remove(job.id(), job);
                if (removed) {
                  LOG.debug("Evicted terminal query job [{}] after TTL", job.id().encode());
                }
              },
              TimeValue.timeValueMillis(ttl.toMillis()),
              GENERIC_POOL);
      pending.put(job.id(), cancellable);
    } catch (RuntimeException e) {
      // Thread pool refused (shutdown, saturation, ...) — remove immediately so the store does
      // not leak. A misbehaving scheduler must not block the state machine.
      LOG.warn(
          "Failed to schedule retention eviction for [{}]; evicting now", job.id().encode(), e);
      store.remove(job.id(), job);
    }
  }
}
