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
  void awaitOutcome_terminalWhenRunnerCompletesBeforeBudget() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 90L);
    job.startRunner();
    CompletionStage<Outcome> stage = job.awaitOutcome(Duration.ofSeconds(30));
    runner.complete(RESULT);
    Outcome outcome = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    Outcome.Terminal terminal = assertInstanceOf(Outcome.Terminal.class, outcome);
    assertSame(RESULT, terminal.result());
  }

  @Test
  void awaitOutcome_pendingWhenBudgetExpiresFirst() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 100L);
    job.startRunner();
    Outcome outcome =
        job.awaitOutcome(Duration.ofMillis(20)).toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertInstanceOf(Outcome.Pending.class, outcome);
    // Runner is still live — a subsequent completion advances the job to SUCCEEDED without
    // affecting the outcome already returned above.
    runner.complete(RESULT);
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
  }

  @Test
  void awaitOutcome_failedCarriesUnwrappedCause() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 110L);
    job.startRunner();
    CompletionStage<Outcome> stage = job.awaitOutcome(Duration.ofSeconds(30));
    IllegalStateException cause = new IllegalStateException("boom");
    runner.fail(cause);
    Outcome outcome = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    Outcome.Failed failed = assertInstanceOf(Outcome.Failed.class, outcome);
    assertSame(cause, failed.cause());
  }

  @Test
  void awaitOutcome_alreadyTerminal_returnsSynchronously() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 120L);
    job.startRunner();
    runner.complete(RESULT);
    Outcome outcome = job.awaitOutcome(Duration.ofSeconds(1)).toCompletableFuture().getNow(null);
    assertNotNull(outcome, "already-terminal job must complete synchronously");
    assertInstanceOf(Outcome.Terminal.class, outcome);
  }

  @Test
  void awaitOutcome_zeroBudget_returnsPendingImmediatelyWhenNotTerminal() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 130L);
    job.startRunner();
    Outcome outcome = job.awaitOutcome(Duration.ZERO).toCompletableFuture().getNow(null);
    assertNotNull(outcome, "zero budget must not block");
    assertInstanceOf(Outcome.Pending.class, outcome);
  }

  @Test
  void awaitOutcome_cancellationSurfacesAsFailed() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 140L);
    job.startRunner();
    CompletionStage<Outcome> stage = job.awaitOutcome(Duration.ofSeconds(30));
    job.cancel();
    Outcome outcome = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    Outcome.Failed failed = assertInstanceOf(Outcome.Failed.class, outcome);
    assertInstanceOf(CancellationException.class, failed.cause());
  }

  @Test
  void awaitOutcome_lateTimerAfterEarlyCompletionDoesNotOverride() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 150L);
    job.startRunner();
    CompletionStage<Outcome> stage = job.awaitOutcome(Duration.ofMillis(50));
    runner.complete(RESULT);
    Outcome outcome = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertInstanceOf(Outcome.Terminal.class, outcome);
    // Give the timer more than the 50ms budget to prove it does not race in later.
    Thread.sleep(120);
    assertSame(outcome, stage.toCompletableFuture().getNow(null));
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

  /** Test double: exposes explicit hooks for the runner's completion future. */
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
