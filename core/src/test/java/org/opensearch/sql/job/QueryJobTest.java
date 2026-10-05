/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

class QueryJobTest {

  private static final QueryJobId ID = new QueryJobId("node-1", "ctx-1");
  private static final Principal OWNER = new Principal("alice", null, List.of());
  private static final Schema SCHEMA = new Schema(List.of());
  private static final QueryResult RESULT =
      new QueryResult(SCHEMA, List.of(), Cursor.None, List.of(), 0);

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
  void success_transitionsToSucceededAndCompletesFuture()
      throws ExecutionException, InterruptedException {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 10L);
    job.startRunner();
    runner.complete(RESULT);
    QueryResult observed = job.completion().toCompletableFuture().get();
    assertEquals(RESULT, observed);
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.SUCCEEDED, status.state());
    assertTrue(status.result().isPresent());
    assertTrue(status.completedAtMillis().isPresent());
    assertTrue(status.startedAtMillis().isPresent());
  }

  @Test
  void failure_transitionsToFailedAndPropagatesCause() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 20L);
    job.startRunner();
    IllegalStateException cause = new IllegalStateException("boom");
    runner.fail(cause);
    ExecutionException ex =
        assertThrows(ExecutionException.class, () -> job.completion().toCompletableFuture().get());
    assertEquals(cause, ex.getCause());
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertEquals("IllegalStateException", status.failure().orElseThrow().type());
  }

  @Test
  void cancel_beforeStart_skipsRunnerAndCompletesExceptionally() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 30L);
    job.cancel();
    job.startRunner();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertFalse(runner.wasRun());
    assertTrue(runner.wasCancelled());
    assertTrue(job.completion().toCompletableFuture().isCompletedExceptionally());
  }

  @Test
  void cancel_afterStart_movesToCancelledAndCancelsRunner() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 40L);
    job.startRunner();
    job.cancel();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertTrue(runner.wasCancelled());
    assertTrue(job.completion().toCompletableFuture().isCompletedExceptionally());
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
  void completion_copyDoesNotAffectJobState() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 60L);
    job.startRunner();
    CompletableFuture<QueryResult> exposed = job.completion().toCompletableFuture();
    // Completing the exposed copy must not drive the internal state machine.
    exposed.complete(RESULT);
    assertEquals(QueryJobState.RUNNING, job.status().state());
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
