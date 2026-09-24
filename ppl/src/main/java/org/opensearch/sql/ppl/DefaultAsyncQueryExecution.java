/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/**
 * Final-result implementation of the asynchronous execution contract.
 *
 * <p>This adapter keeps the existing callback-based query execution unchanged. Until partial-result
 * producers are added, {@link #currentResult()} is empty while execution is running and exposes the
 * final response immediately before successful completion is published.
 */
final class DefaultAsyncQueryExecution
    implements AsyncQueryExecution, ResponseListener<QueryResponse> {
  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private volatile QueryResponse finalResult;

  @Override
  public void onResponse(QueryResponse response) {
    finalResult = Objects.requireNonNull(response);
    completion.complete(null);
  }

  @Override
  public void onFailure(Exception failure) {
    completion.completeExceptionally(Objects.requireNonNull(failure));
  }

  @Override
  public Optional<QueryResponse> currentResult() {
    return Optional.ofNullable(finalResult);
  }

  @Override
  public CompletionStage<Void> completion() {
    return completion;
  }

  @Override
  public void close() {}
}
