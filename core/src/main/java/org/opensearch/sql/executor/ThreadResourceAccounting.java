/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

/**
 * Per-query accounting of the CPU and memory a query uses on the SQL plugin's own threads.
 *
 * <p>OpenSearch's task resource tracking only brackets threads on its task-aware pools, which a
 * plugin pool cannot opt into, so the engine brackets its own threads instead. It does so through
 * this interface rather than calling core directly: whether and how a stretch of work is measured
 * is the task's decision, not the engine's.
 *
 * <p>It lives in core although only the opensearch module calls it ({@code OpenSearchQueryManager},
 * {@code ThreadPoolExecutionDispatcher} and {@code BackgroundSearchScanner}), because {@code
 * PPLQueryTask} implements it and must not reference opensearch module types: plugins such as
 * alerting load that task with only core, ppl and protocol (#5923, checked by {@code
 * ThinJarClassesTest}). The plugin module, where the task lives, is not visible to the opensearch
 * module either.
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
