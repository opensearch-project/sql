/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

/**
 * Per-query accounting of the CPU and memory a query uses on the SQL plugin's own threads.
 *
 * <p>Core's task resource tracking only brackets threads on its task-aware pools, which a plugin
 * pool cannot opt into, so the engine brackets its own threads instead. It does so through this
 * interface rather than {@code Task.startThreadResourceTracking}, so nothing is written into the
 * task's {@code resource_stats}: whether and how to measure is the task's decision, not the
 * engine's.
 */
public interface ThreadResourceAccounting {

  /**
   * Starts measuring the current thread for this query. The returned scope must be closed on the
   * same thread when the thread's work for the query ends.
   */
  Scope enterThread();

  /** Opens a scope for {@code task}, or a no-op scope if the task does no accounting. */
  static Scope enter(Object task) {
    return task instanceof ThreadResourceAccounting accounting
        ? accounting.enterThread()
        : Scope.NOOP;
  }

  /** A measured stretch of one thread's work; closing it records the usage. */
  interface Scope extends AutoCloseable {
    Scope NOOP = () -> {};

    @Override
    void close();
  }
}
