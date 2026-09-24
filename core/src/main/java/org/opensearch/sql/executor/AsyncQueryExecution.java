/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/**
 * Lifecycle-facing handle for one asynchronous query execution.
 *
 * <p>The execution module owns result production and execution-specific resources. The lifecycle
 * module owns this handle after submission and uses it to read the current result, observe terminal
 * completion, and release those resources.
 *
 * <p>On successful completion, the authoritative final result must be visible through {@link
 * #currentResult()} before {@link #completion()} completes normally. Implementations must make
 * {@link #close()} idempotent and safe to call concurrently with {@link #currentResult()}.
 */
public interface AsyncQueryExecution extends AutoCloseable {

  /** Returns the complete result currently visible, or empty before a result is available. */
  Optional<QueryResponse> currentResult();

  /** Completes normally on query success and exceptionally on query failure. */
  CompletionStage<Void> completion();

  /** Releases execution-owned result resources. */
  @Override
  void close();
}
