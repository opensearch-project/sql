/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
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

  public RetentionPolicy(QueryJobStore store, ThreadPool threadPool) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
  }

  /**
   * Retains a job exposed for polling. When the job reaches a terminal state, a delayed task on the
   * generic pool removes it from the store after {@code ttl}. Completion before registration is
   * handled by {@link QueryJob#onTerminal(Runnable)}, which invokes the callback immediately for an
   * already-terminal job. Inline submission outcomes never arm retention.
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

  private void scheduleEviction(QueryJob job, Duration ttl) {
    QueryJobId id = job.id();
    // The eviction timer must not retain a deleted job until TTL.
    WeakReference<QueryJob> retained = new WeakReference<>(job);
    try {
      threadPool.schedule(
          () -> {
            QueryJob current = retained.get();
            if (current != null && store.remove(id, current)) {
              LOG.debug("Evicted terminal query job [{}] after TTL", id.encode());
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
