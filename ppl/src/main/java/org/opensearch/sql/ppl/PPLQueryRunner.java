/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.QueryRunner;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

/**
 * PPL adapter for the neutral {@link QueryRunner} SPI.
 *
 * <p>Wraps a single {@link PPLService#execute} call. Result production runs on the PPL query
 * manager's worker threads exactly as it does today; this class only bridges the callback-based
 * response into a {@link CompletionStage}.
 *
 * <p>Cancellation is cooperative: {@link #cancel()} marks the future as cancelled so a late
 * response drops on the floor, and invokes the caller-supplied hook so the engine can stop the
 * in-flight execution at its next cancellation check.
 */
public final class PPLQueryRunner implements QueryRunner {

  private final PPLService pplService;
  private final PPLQueryRequest request;
  private final Consumer<String> anonymizedQuerySink;
  private final Clock clock;
  private final Runnable cancelExecution;
  private final AtomicBoolean started = new AtomicBoolean();
  private final CompletableFuture<QueryResult> future = new CompletableFuture<>();

  /**
   * @param pplService live PPL service; not owned by the runner
   * @param request rich PPL request; carries include_metadata, time_bounds, format, etc.
   * @param anonymizedQuerySink receives the PII-scrubbed query text; supply {@link
   *     PPLService#NO_ANONYMIZED_QUERY_SINK} when no telemetry is wanted
   * @param clock time source; used to measure {@code tookMillis}
   * @param cancelExecution stops the engine's in-flight execution; invoked at most once, only when
   *     {@link #cancel()} wins against completion
   * @throws NullPointerException if any argument is {@code null}
   */
  public PPLQueryRunner(
      PPLService pplService,
      PPLQueryRequest request,
      Consumer<String> anonymizedQuerySink,
      Clock clock,
      Runnable cancelExecution) {
    this.pplService = Objects.requireNonNull(pplService, "pplService must not be null");
    this.request = Objects.requireNonNull(request, "request must not be null");
    this.anonymizedQuerySink =
        Objects.requireNonNull(anonymizedQuerySink, "anonymizedQuerySink must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.cancelExecution =
        Objects.requireNonNull(cancelExecution, "cancelExecution must not be null");
  }

  @Override
  public CompletionStage<QueryResult> run() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("PPLQueryRunner is single-use");
    }
    long startMillis = clock.millis();
    pplService.execute(
        request,
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            future.complete(QueryResult.of(response, clock.millis() - startMillis));
          }

          @Override
          public void onFailure(Exception e) {
            future.completeExceptionally(e);
          }
        },
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            future.complete(QueryResult.of(response, clock.millis() - startMillis));
          }

          @Override
          public void onFailure(Exception e) {
            future.completeExceptionally(e);
          }
        },
        anonymizedQuerySink);
    return future;
  }

  @Override
  public void cancel() {
    // Unlike cancel(false), this returns true only for the call that performs the transition.
    if (future.completeExceptionally(new CancellationException("PPL query cancelled"))) {
      cancelExecution.run();
    }
  }
}
