/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Retained asynchronous PPL jobs on the owner node.
 *
 * <p>The registry is a thin wrapper around a concurrent map. It exposes the four operations {@link
 * QueryJob} needs to make itself discoverable, and no more: capacity accounting, listener
 * notification, and task management remain inside {@link QueryJob}. All methods are safe for
 * concurrent use.
 *
 * <p>Only running jobs are registered. A job becomes visible when {@link QueryJob#create} publishes
 * it and is removed on completion, DELETE, expiry, or shutdown. On {@link #close}, the registry
 * stops accepting new jobs and asks each remaining job to release its resources.
 */
public final class QueryJobRegistry implements Closeable {
  private final ConcurrentMap<QueryJobId, QueryJob> jobs = new ConcurrentHashMap<>();
  private volatile boolean closed;

  /**
   * Publishes a job under its opaque ID.
   *
   * <p>If a job with the same ID is already registered, the existing job is returned and the caller
   * is expected to abort the incoming duplicate. In practice, {@link QueryJobId#create} draws its
   * context ID from {@link java.util.UUID#randomUUID} and collisions are not observed.
   *
   * @param job job to publish; its ID is used as the registry key
   * @return existing job if the ID was already registered, {@code null} on successful insertion
   * @throws IllegalStateException when the registry has been closed
   */
  public QueryJob add(QueryJob job) {
    Objects.requireNonNull(job);
    if (closed) {
      throw new IllegalStateException("PPL asynchronous query registry is closed");
    }
    return jobs.putIfAbsent(job.getJobId(), job);
  }

  /**
   * Looks up a job by opaque ID.
   *
   * @param id opaque ID assigned by this node
   * @return job when the ID resolves locally, or empty when the ID is unknown
   */
  public Optional<QueryJob> get(QueryJobId id) {
    Objects.requireNonNull(id);
    return Optional.ofNullable(jobs.get(id));
  }

  /**
   * Removes a job only when the given ID still resolves to the given job instance.
   *
   * @param id opaque ID assigned by this node
   * @param job job that expects to still hold the registration
   * @return {@code true} when the mapping was removed
   */
  public boolean remove(QueryJobId id, QueryJob job) {
    Objects.requireNonNull(id);
    Objects.requireNonNull(job);
    return jobs.remove(id, job);
  }

  /**
   * Snapshot of every job currently registered.
   *
   * <p>Iteration order is unspecified. The returned collection is a defensive copy and does not
   * reflect later registry mutations.
   *
   * @return jobs registered at the moment of the call
   */
  public Collection<QueryJob> jobs() {
    return List.copyOf(jobs.values());
  }

  /**
   * Stops accepting new jobs and asks every registered job to release its resources.
   *
   * <p>Each surviving job receives {@link QueryJob#discard} with a shutdown reason. The registry
   * itself becomes idempotent: subsequent calls are no-ops and any future {@link #add} throws.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (QueryJob job : jobs.values()) {
      job.discard("PPL asynchronous query service is closing");
    }
  }
}
