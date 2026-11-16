/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntSupplier;
import org.opensearch.sql.common.error.ResourceLimitExceededException;
import org.opensearch.sql.common.setting.Settings;

/**
 * Node-local, in-memory {@link QueryJobStore}.
 *
 * <p>The store retains at most a maximum number of jobs. A job is retained by {@link
 * #retain(QueryJob)}, before its id is returned for polling, whether it is still running or has
 * finished, and stays retained until it is removed. {@link #register(QueryJob)} refuses a job while
 * the maximum is retained, and {@link #retain(QueryJob)} refuses one that would exceed it. The
 * limit is read on every call, so a lowered limit applies to the next submit and retained jobs are
 * not evicted. Cluster visibility is intentionally not modelled.
 */
public final class InMemoryQueryJobStore implements QueryJobStore {

  private final ConcurrentMap<QueryJobId, QueryJob> jobs = new ConcurrentHashMap<>();
  private final IntSupplier maxRetainedJobs;

  // Guarded by this.
  private boolean closed;
  private final Set<QueryJob> retained = Collections.newSetFromMap(new IdentityHashMap<>());

  /** Creates a store without a limit. */
  public InMemoryQueryJobStore() {
    this(() -> Integer.MAX_VALUE);
  }

  /**
   * @param maxRetainedJobs maximum number of jobs the store retains; read on every {@link
   *     #register} and {@link #retain}
   */
  public InMemoryQueryJobStore(IntSupplier maxRetainedJobs) {
    this.maxRetainedJobs =
        Objects.requireNonNull(maxRetainedJobs, "maxRetainedJobs must not be null");
  }

  /**
   * {@inheritDoc}
   *
   * @throws ResourceLimitExceededException when the store retains the maximum number of jobs
   */
  @Override
  public synchronized QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    if (closed) {
      throw new IllegalStateException("Query job store is closed");
    }
    checkCapacity();
    return jobs.putIfAbsent(job.id(), job);
  }

  @Override
  public synchronized void retain(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    if (jobs.get(job.id()) != job || retained.contains(job)) {
      return;
    }
    checkCapacity();
    retained.add(job);
  }

  private void checkCapacity() {
    if (retained.size() >= maxRetainedJobs.getAsInt()) {
      throw new ResourceLimitExceededException(
          String.format(
              Locale.ROOT,
              "Too many asynchronous queries are retained on this node [%s]; retry later, or"
                  + " delete queries you no longer need with DELETE /_plugins/_async_query/{id}",
              Settings.Key.QUERY_ASYNC_MAX_RETAINED_JOBS.getKeyValue()));
    }
  }

  @Override
  public Optional<QueryJob> find(QueryJobId id) {
    Objects.requireNonNull(id, "id must not be null");
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public synchronized boolean remove(QueryJobId id, QueryJob job) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(job, "job must not be null");
    if (!jobs.remove(id, job)) {
      return false;
    }
    retained.remove(job);
    return true;
  }

  @Override
  public Collection<QueryJob> jobs() {
    return List.copyOf(jobs.values());
  }

  @Override
  public void close() {
    List<QueryJob> residual;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      residual = List.copyOf(jobs.values());
      jobs.clear();
      retained.clear();
    }
    // Cancelled outside the lock: a job's terminal callbacks may call back into the store.
    for (QueryJob job : residual) {
      job.cancel();
    }
  }
}
