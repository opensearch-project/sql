/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.opensearch.threadpool.ThreadPool;

class RetentionPolicyTest {

  private static final Duration TTL = Duration.ofMinutes(5);

  @Test
  void arm_schedulesEvictionOnTerminalTransition() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    ThreadPool threadPool = mock(ThreadPool.class);
    // Fire the scheduled runnable inline so eviction happens synchronously.
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(), anyString());

    RetentionPolicy policy = new RetentionPolicy(store, threadPool);
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(new QueryJobId("node", "ctx"), Principal.UNSECURED, runner, Clock.systemUTC());
    store.register(job);
    policy.arm(job, TTL);
    job.startRunner();

    runner.complete();
    assertEquals(Optional.empty(), store.find(job.id()));
  }

  @Test
  void arm_evictsImmediatelyWhenSchedulerRejects() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    ThreadPool threadPool = mock(ThreadPool.class);
    doThrow(new IllegalStateException("shutdown"))
        .when(threadPool)
        .schedule(any(Runnable.class), any(), anyString());

    RetentionPolicy policy = new RetentionPolicy(store, threadPool);
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(new QueryJobId("node", "ctx"), Principal.UNSECURED, runner, Clock.systemUTC());
    store.register(job);
    policy.arm(job, TTL);
    job.startRunner();

    runner.complete();
    assertFalse(store.find(job.id()).isPresent());
  }

  @Test
  void arm_rejectsNonPositiveTtl() {
    RetentionPolicy policy =
        new RetentionPolicy(new InMemoryQueryJobStore(), mock(ThreadPool.class));
    QueryJob job =
        new QueryJob(
            new QueryJobId("node", "ctx"),
            Principal.UNSECURED,
            new RecordingRunner(),
            Clock.systemUTC());
    assertThrows(IllegalArgumentException.class, () -> policy.arm(job, Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> policy.arm(job, Duration.ofSeconds(-1)));
  }

  private static final class RecordingRunner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();

    @Override
    public CompletionStage<QueryResult> run() {
      return future;
    }

    @Override
    public void cancel() {}

    void complete() {
      future.complete(
          new QueryResult.Rows(
              new org.opensearch.sql.executor.ExecutionEngine.Schema(java.util.List.of()),
              java.util.List.of(),
              org.opensearch.sql.executor.pagination.Cursor.None,
              java.util.List.of(),
              0));
    }
  }
}
