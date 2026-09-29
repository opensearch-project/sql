/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.concurrent.CompletionStage;

/**
 * Engine adapter that produces a {@link QueryResult} for one submitted query.
 *
 * <p>Implementations live in the engine modules (PPL, SQL, analytics-engine). They translate the
 * neutral {@link SubmitRequest} into the engine-specific execution plan, own any threading, and
 * report completion or failure through the returned {@link CompletionStage}.
 *
 * <p>A runner is single-use. {@link #run()} must be invoked exactly once; subsequent invocations
 * throw {@link IllegalStateException}. {@link #cancel()} is idempotent and safe to invoke before
 * {@link #run()} or after completion.
 */
public interface QueryRunner {

  /** Starts execution and returns the future that carries the final result. */
  CompletionStage<QueryResult> run();

  /** Requests cooperative cancellation. Safe to call from any state. */
  void cancel();
}
