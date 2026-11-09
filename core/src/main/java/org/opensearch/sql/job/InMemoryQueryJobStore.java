/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Node-local, in-memory {@link QueryJobStore}.
 *
 * <p>This is the MVP implementation. Its footprint is a single {@link ConcurrentHashMap}; capacity,
 * eviction, and cluster visibility are intentionally not modelled. Any of them can be added later
 * behind the {@code QueryJobStore} interface without changing {@link QueryJob} or {@link
 * QueryJobService}.
 */
public final class InMemoryQueryJobStore implements QueryJobStore {

  private final ConcurrentMap<QueryJobId, QueryJob> jobs = new ConcurrentHashMap<>();
  private volatile boolean closed;

  @Override
  public QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    if (closed) {
      throw new IllegalStateException("Query job store is closed");
    }
    return jobs.putIfAbsent(job.id(), job);
  }

  @Override
  public Optional<QueryJob> find(QueryJobId id) {
    Objects.requireNonNull(id, "id must not be null");
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public boolean remove(QueryJobId id, QueryJob job) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(job, "job must not be null");
    return jobs.remove(id, job);
  }

  @Override
  public Collection<QueryJob> jobs() {
    return List.copyOf(jobs.values());
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
