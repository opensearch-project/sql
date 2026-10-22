/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.threadpool.ThreadPool;

class RetentionPolicyTest {

  private static final Duration TTL = Duration.ofMinutes(5);

  @Test
  void arm_schedulesEvictionOnlyAfterTerminalTransition() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    ThreadPool threadPool = mock(ThreadPool.class);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(), anyString());
    RetentionPolicy policy = new RetentionPolicy(store, threadPool);
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner);
    store.register(job);
    policy.arm(job, TTL);
    job.startRunner();
    verifyNoInteractions(threadPool);

    runner.complete();

    assertFalse(store.find(job.id()).isPresent());
  }

  @Test
  void arm_afterCompletionStillSchedulesEviction() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    ThreadPool threadPool = mock(ThreadPool.class);
    RetentionPolicy policy = new RetentionPolicy(store, threadPool);
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner);
    store.register(job);
    job.startRunner();
    runner.complete();

    policy.arm(job, TTL);

    ArgumentCaptor<Runnable> eviction = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool).schedule(eviction.capture(), any(), anyString());
    eviction.getValue().run();
    assertFalse(store.find(job.id()).isPresent());
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
    QueryJob job = newJob(runner);
    store.register(job);
    policy.arm(job, TTL);
    job.startRunner();

    runner.complete();

    assertFalse(store.find(job.id()).isPresent());
  }

  @Test
  void eviction_doesNotRemoveDifferentJobRegisteredUnderSameId() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    ThreadPool threadPool = mock(ThreadPool.class);
    RetentionPolicy policy = new RetentionPolicy(store, threadPool);
    RecordingRunner runner = new RecordingRunner();
    QueryJob original = newJob(runner);
    store.register(original);
    policy.arm(original, TTL);
    original.startRunner();
    runner.complete();
    ArgumentCaptor<Runnable> eviction = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool).schedule(eviction.capture(), any(), anyString());
    store.remove(original.id(), original);
    QueryJob replacement = newJob(new RecordingRunner());
    store.register(replacement);

    eviction.getValue().run();

    assertSame(replacement, store.find(original.id()).orElseThrow());
  }

  @Test
  void arm_rejectsNonPositiveTtl() {
    RetentionPolicy policy =
        new RetentionPolicy(new InMemoryQueryJobStore(), mock(ThreadPool.class));
    QueryJob job = newJob(new RecordingRunner());
    assertThrows(IllegalArgumentException.class, () -> policy.arm(job, Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> policy.arm(job, Duration.ofSeconds(-1)));
  }

  private static QueryJob newJob(QueryRunner runner) {
    return new QueryJob(
        new QueryJobId("node", "ctx"), Principal.UNSECURED, runner, Clock.systemUTC());
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
              new ExecutionEngine.Schema(List.of()), List.of(), Cursor.None, List.of(), 0));
    }
  }
}
