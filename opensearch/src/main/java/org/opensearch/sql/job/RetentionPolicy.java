/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Duration;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

/**
 * Evicts terminal jobs from a {@link QueryJobStore} after a fixed TTL.
 *
 * <p>Retention lives at the service layer, not on {@link QueryJob}. The state machine has no
 * retention axis (see the design in #5818); retention is orthogonal to state. Terminal jobs are
 * kept just long enough for a client to observe them, then dropped so the store's memory footprint
 * stays bounded.
 *
 * <p>This is the MVP implementation: a single fixed TTL for every job, armed at submission time,
 * fired via {@link ThreadPool#schedule(Runnable, TimeValue, String)}. A follow-up will add
 * per-request {@code keep_alive} override and GET-refreshes-lease semantics.
 */
public final class RetentionPolicy {

  private static final Logger LOG = LogManager.getLogger(RetentionPolicy.class);
  private static final String GENERIC_POOL = ThreadPool.Names.GENERIC;

  private final QueryJobStore store;
  private final ThreadPool threadPool;
  private final Duration ttl;

  /**
   * @param store store to evict from
   * @param threadPool node thread pool; retention timers fire on {@link ThreadPool.Names#GENERIC}
   * @param ttl duration to retain a job after it enters a terminal state; must be positive
   * @throws NullPointerException if any argument is {@code null}
   * @throws IllegalArgumentException if {@code ttl} is not positive
   */
  public RetentionPolicy(QueryJobStore store, ThreadPool threadPool, Duration ttl) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
    if (ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
  }

  /**
   * Attaches an eviction timer to {@code job}'s completion. When the job reaches a terminal state,
   * a delayed task on the generic pool removes it from the store after {@link #ttl}. Idempotent
   * against the store: the removal is conditional on the same job still being registered.
   *
   * @param job the newly-submitted job; must already be registered in the store
   */
  public void arm(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    job.onTerminal(() -> scheduleEviction(job));
  }

  private void scheduleEviction(QueryJob job) {
    try {
      threadPool.schedule(
          () -> {
            boolean removed = store.remove(job.id(), job);
            if (removed) {
              LOG.debug("Evicted terminal query job [{}] after TTL", job.id().encode());
            }
          },
          TimeValue.timeValueMillis(ttl.toMillis()),
          GENERIC_POOL);
    } catch (RuntimeException e) {
      // Thread pool refused (shutdown, saturation, ...) — remove immediately so the store does
      // not leak. A misbehaving scheduler must not block the state machine.
      LOG.warn(
          "Failed to schedule retention eviction for [{}]; evicting now", job.id().encode(), e);
      store.remove(job.id(), job);
    }
  }
}
