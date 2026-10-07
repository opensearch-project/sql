/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

class QueryJobTest {

  private static final QueryJobId ID = new QueryJobId("node-1", "ctx-1");
  private static final Principal OWNER = new Principal("alice", null, List.of());
  private static final Schema SCHEMA = new Schema(List.of());
  private static final QueryResult RESULT =
      new QueryResult.Rows(SCHEMA, List.of(), Cursor.None, List.of(), 0);

  @Test
  void status_startsInPending() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(ID, OWNER, runner, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.PENDING, status.state());
    assertEquals(1_000L, status.submittedAtMillis());
    assertTrue(status.startedAtMillis().isEmpty());
    assertTrue(status.completedAtMillis().isEmpty());
  }

  @Test
  void success_transitionsToSucceededAndFiresOnTerminal() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 10L);
    AtomicInteger fired = new AtomicInteger();
    job.onTerminal(fired::incrementAndGet);
    job.startRunner();
    runner.complete(RESULT);
    assertEquals(1, fired.get());
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.SUCCEEDED, status.state());
    assertTrue(status.result().isPresent());
    assertTrue(status.completedAtMillis().isPresent());
    assertTrue(status.startedAtMillis().isPresent());
  }

  @Test
  void failure_transitionsToFailedAndCarriesTypedFailure() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 20L);
    job.startRunner();
    IllegalStateException cause = new IllegalStateException("boom");
    runner.fail(cause);
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertEquals("IllegalStateException", status.failure().orElseThrow().type());
    assertEquals(java.util.Map.of(), status.failure().orElseThrow().details());
  }

  @Test
  void failure_appliesRendererForStructuredDetails() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(
            ID,
            OWNER,
            runner,
            Clock.fixed(Instant.ofEpochMilli(25L), ZoneOffset.UTC),
            t -> java.util.Map.of("code", "FIELD_NOT_FOUND", "reason", t.getMessage()));
    job.startRunner();
    runner.fail(new IllegalArgumentException("Field [x] not found."));
    QueryFailure failure = job.status().failure().orElseThrow();
    assertEquals("FIELD_NOT_FOUND", failure.details().get("code"));
    assertEquals("Field [x] not found.", failure.details().get("reason"));
  }

  @Test
  void failure_rendererBugDoesNotBlockTerminalTransition() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(
            ID,
            OWNER,
            runner,
            Clock.fixed(Instant.ofEpochMilli(26L), ZoneOffset.UTC),
            t -> {
              throw new RuntimeException("renderer bug");
            });
    job.startRunner();
    runner.fail(new IllegalStateException("boom"));
    assertEquals(QueryJobState.FAILED, job.status().state());
    assertEquals(java.util.Map.of(), job.status().failure().orElseThrow().details());
  }

  @Test
  void cancel_beforeStart_skipsRunnerAndFiresOnTerminal() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 30L);
    AtomicInteger fired = new AtomicInteger();
    job.onTerminal(fired::incrementAndGet);
    job.cancel();
    job.startRunner();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertFalse(runner.wasRun());
    assertTrue(runner.wasCancelled());
    assertEquals(1, fired.get());
  }

  @Test
  void cancel_afterStart_movesToCancelledAndCancelsRunner() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 40L);
    job.startRunner();
    job.cancel();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertTrue(runner.wasCancelled());
  }

  @Test
  void cancel_isIdempotentInTerminalState() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 50L);
    job.startRunner();
    runner.complete(RESULT);
    QueryJobStatus terminal = job.status();
    job.cancel();
    assertEquals(terminal, job.status());
  }

  @Test
  void startRunner_secondInvocation_isNoOp() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 70L);
    job.startRunner();
    job.startRunner();
    assertEquals(1, runner.runInvocations());
  }

  @Test
  void nullResultFromRunner_transitionsToFailed() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 80L);
    job.startRunner();
    runner.complete(null);
    assertEquals(QueryJobState.FAILED, job.status().state());
  }

  @Test
  void await_returnsRunnerResultWhenCompletesBeforeBudget() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 90L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(30));
    runner.complete(RESULT);
    assertSame(RESULT, stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
  }

  @Test
  void await_returnsRunningWhenBudgetExpiresFirst() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 100L);
    job.startRunner();
    QueryResult result =
        job.await(Duration.ofMillis(20)).toCompletableFuture().get(1, TimeUnit.SECONDS);
    QueryResult.Running running = assertInstanceOf(QueryResult.Running.class, result);
    assertEquals(ID, running.id());
    runner.complete(RESULT);
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
  }

  @Test
  void await_completesExceptionallyWithUnwrappedCause() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 110L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(30));
    IllegalStateException cause = new IllegalStateException("boom");
    runner.fail(cause);
    ExecutionException ex =
        assertThrows(
            ExecutionException.class, () -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertSame(cause, ex.getCause());
  }

  @Test
  void await_alreadyTerminal_returnsSynchronously() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 120L);
    job.startRunner();
    runner.complete(RESULT);
    QueryResult result = job.await(Duration.ofSeconds(1)).toCompletableFuture().getNow(null);
    assertNotNull(result, "already-terminal job must complete synchronously");
    assertSame(RESULT, result);
  }

  @Test
  void await_zeroBudget_returnsRunningImmediately() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 130L);
    job.startRunner();
    QueryResult result = job.await(Duration.ZERO).toCompletableFuture().getNow(null);
    assertNotNull(result, "zero budget must not block");
    assertInstanceOf(QueryResult.Running.class, result);
  }

  @Test
  void await_cancellationSurfacesAsExceptionalCompletion() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 140L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(30));
    job.cancel();
    ExecutionException ex =
        assertThrows(
            ExecutionException.class, () -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertInstanceOf(CancellationException.class, ex.getCause());
  }

  @Test
  void await_lateTimerAfterEarlyCompletionDoesNotOverride() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 150L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofMillis(50));
    runner.complete(RESULT);
    QueryResult result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertSame(RESULT, result);
    Thread.sleep(120);
    assertSame(result, stage.toCompletableFuture().getNow(null));
  }

  @Test
  void onTerminal_rejectsNull() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 160L);
    assertThrows(NullPointerException.class, () -> job.onTerminal(null));
  }

  private static QueryJob newJob(QueryRunner runner, long submittedMillis) {
    return new QueryJob(
        ID, OWNER, runner, Clock.fixed(Instant.ofEpochMilli(submittedMillis), ZoneOffset.UTC));
  }

  private static final class RecordingRunner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private int runInvocations;
    private boolean cancelled;

    @Override
    public CompletionStage<QueryResult> run() {
      runInvocations++;
      return future;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }

    void complete(QueryResult result) {
      future.complete(result);
    }

    void fail(Throwable throwable) {
      future.completeExceptionally(throwable);
    }

    boolean wasRun() {
      return runInvocations > 0;
    }

    boolean wasCancelled() {
      return cancelled;
    }

    int runInvocations() {
      return runInvocations;
    }
  }
}
