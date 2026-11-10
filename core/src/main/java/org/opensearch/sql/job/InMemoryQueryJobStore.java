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
 * <p>{@link #register(QueryJob)} refuses a job while the store retains the maximum number of
 * results. A job's result is retained from the moment the job finishes until the job is removed;
 * the service removes a job as soon as its result is returned inline, so only results kept for
 * polling stay counted. Running jobs are not counted, so jobs admitted while there was room can
 * take the count past its limit when they finish. The limit is read on every register, so a lowered
 * limit applies to the next submit and retained results are not evicted. Cluster visibility is
 * intentionally not modelled.
 */
public final class InMemoryQueryJobStore implements QueryJobStore {

  private final ConcurrentMap<QueryJobId, QueryJob> jobs = new ConcurrentHashMap<>();
  private final IntSupplier maxRetainedResults;
  private volatile boolean closed;

  // Registered jobs that have finished, whose results the store retains; guarded by this.
  private final Set<QueryJob> results = Collections.newSetFromMap(new IdentityHashMap<>());

  /** Creates a store without a limit. */
  public InMemoryQueryJobStore() {
    this(() -> Integer.MAX_VALUE);
  }

  /**
   * @param maxRetainedResults maximum number of results the store retains; read on every {@link
   *     #register}
   */
  public InMemoryQueryJobStore(IntSupplier maxRetainedResults) {
    this.maxRetainedResults =
        Objects.requireNonNull(maxRetainedResults, "maxRetainedResults must not be null");
  }

  /**
   * {@inheritDoc}
   *
   * @throws ResourceLimitExceededException when the store retains the maximum number of results
   */
  @Override
  public QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    if (closed) {
      throw new IllegalStateException("Query job store is closed");
    }
    int limit = maxRetainedResults.getAsInt();
    QueryJob existing;
    synchronized (this) {
      if (results.size() >= limit) {
        throw new ResourceLimitExceededException(
            String.format(
                Locale.ROOT,
                "Too many asynchronous query results are retained on this node [%s]; retry later,"
                    + " or delete queries you no longer need with DELETE"
                    + " /_plugins/_async_query/{id}",
                Settings.Key.QUERY_ASYNC_MAX_RETAINED_RESULTS.getKeyValue()));
      }
      existing = jobs.putIfAbsent(job.id(), job);
    }
    if (existing == null) {
      job.onTerminal(() -> retainResult(job));
    }
    return existing;
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
    results.remove(job);
    return true;
  }

  @Override
  public Collection<QueryJob> jobs() {
    return List.copyOf(jobs.values());
  }

  private synchronized void retainResult(QueryJob job) {
    // A job removed before it finished retains no result.
    if (jobs.get(job.id()) == job) {
      results.add(job);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (QueryJob job : jobs.values()) {
      job.cancel();
    }
    jobs.clear();
  }
}
