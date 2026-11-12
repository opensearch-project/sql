/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.error.ResourceLimitExceededException;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

class InMemoryQueryJobStoreTest {

  private static final QueryResult RESULT =
      new QueryResult.Rows(new Schema(List.of()), List.of(), Cursor.None, List.of(), 0);

  @Test
  void register_returnsNullOnFirstInsertion() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-1");
    assertEquals(null, store.register(job));
    assertSame(job, store.find(job.id()).orElseThrow());
  }

  @Test
  void register_returnsExistingJobOnDuplicateId() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob first = newJob("ctx-duplicate");
    QueryJob second =
        new QueryJob(first.id(), Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
    assertEquals(null, store.register(first));
    assertSame(first, store.register(second));
  }

  @Test
  void remove_isConditional() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-remove");
    store.register(job);
    QueryJob other =
        new QueryJob(job.id(), Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
    assertFalse(store.remove(job.id(), other));
    assertTrue(store.remove(job.id(), job));
    assertEquals(Optional.empty(), store.find(job.id()));
  }

  @Test
  void jobs_returnsDefensiveCopy() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-jobs");
    store.register(job);
    assertNotNull(store.jobs());
    assertEquals(1, store.jobs().size());
    store.remove(job.id(), job);
    assertEquals(0, store.jobs().size());
  }

  @Test
  void close_rejectsFurtherRegistration() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    store.close();
    QueryJob job = newJob("ctx-after-close");
    assertThrows(IllegalStateException.class, () -> store.register(job));
  }

  @Test
  void close_cancelsResidualJobs() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-shutdown");
    store.register(job);
    store.close();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
  }

  @Test
  void register_refusesWhileTheLimitIsReached() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    QueryJob finished = finishedJob(store, "ctx-finished");
    ResourceLimitExceededException e =
        assertThrows(
            ResourceLimitExceededException.class, () -> store.register(newJob("ctx-second")));
    assertTrue(e.getMessage().contains("plugins.query.async.max_retained_results"), e.getMessage());
    assertTrue(store.remove(finished.id(), finished));
    assertEquals(null, store.register(newJob("ctx-second")));
  }

  @Test
  void register_readsTheLimitOnEveryCall() {
    AtomicInteger limit = new AtomicInteger(2);
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(limit::get);
    finishedJob(store, "ctx-1");
    limit.set(1);
    assertThrows(ResourceLimitExceededException.class, () -> store.register(newJob("ctx-2")));
  }

  @Test
  void runningJob_isNotCountedUntilItFinishes() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    CompletableFuture<QueryResult> result = new CompletableFuture<>();
    startedJob(store, "ctx-running", result);
    assertTrue(admitsAnotherJob(store));
    result.complete(RESULT);
    assertFalse(admitsAnotherJob(store));
  }

  @Test
  void jobRemovedBeforeItFinishes_isNotCounted() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    QueryJob job = startedJob(store, "ctx-deleted", new CompletableFuture<>());
    store.remove(job.id(), job);
    job.cancel();
    assertTrue(admitsAnotherJob(store));
  }

  @Test
  void failedJob_isCountedLikeASucceededOne() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    CompletableFuture<QueryResult> result = new CompletableFuture<>();
    startedJob(store, "ctx-failed", result);
    result.completeExceptionally(new IllegalStateException("boom"));
    assertFalse(admitsAnotherJob(store));
  }

  @Test
  void jobsRemovedWhileTheyFinish_areCountedExactlyOnce() throws Exception {
    AtomicInteger limit = new AtomicInteger(Integer.MAX_VALUE);
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(limit::get);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 2000; i++) {
        CompletableFuture<QueryResult> result = new CompletableFuture<>();
        QueryJob job = startedJob(store, "ctx-" + i, result);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> finish = pool.submit(() -> await(start, () -> result.complete(RESULT)));
        Future<?> remove = pool.submit(() -> await(start, () -> store.remove(job.id(), job)));
        start.countDown();
        finish.get(10, TimeUnit.SECONDS);
        remove.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    limit.set(1);
    // Nothing stays counted after every job was removed...
    assertTrue(admitsAnotherJob(store));
    // ...and nothing was released twice, so one finished job still fills a limit of one.
    finishedJob(store, "ctx-last");
    assertFalse(admitsAnotherJob(store));
  }

  private static void await(CountDownLatch start, Runnable action) {
    try {
      start.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    action.run();
  }

  /** Registers and starts a job whose runner completes with {@code result}. */
  private static QueryJob startedJob(
      InMemoryQueryJobStore store, String contextId, CompletableFuture<QueryResult> result) {
    QueryJob job =
        new QueryJob(
            new QueryJobId("node-1", contextId),
            Principal.UNSECURED,
            new FutureRunner(result),
            Clock.systemUTC());
    assertEquals(null, store.register(job));
    job.startRunner();
    return job;
  }

  private static QueryJob finishedJob(InMemoryQueryJobStore store, String contextId) {
    CompletableFuture<QueryResult> result = new CompletableFuture<>();
    QueryJob job = startedJob(store, contextId, result);
    result.complete(RESULT);
    return job;
  }

  /** Registers and removes a fresh job; returns whether the store admitted it. */
  private static boolean admitsAnotherJob(InMemoryQueryJobStore store) {
    QueryJob probe = newJob("probe-" + UUID.randomUUID());
    try {
      store.register(probe);
    } catch (ResourceLimitExceededException e) {
      return false;
    }
    store.remove(probe.id(), probe);
    return true;
  }

  private static QueryJob newJob(String contextId) {
    QueryJobId id = new QueryJobId("node-1", contextId);
    return new QueryJob(id, Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
  }

  private record FutureRunner(CompletableFuture<QueryResult> result) implements QueryRunner {
    @Override
    public CompletionStage<QueryResult> run() {
      return result;
    }

    @Override
    public void cancel() {
      result.cancel(false);
    }
  }

  private static final class NoopRunner implements QueryRunner {
    @Override
    public CompletionStage<QueryResult> run() {
      return new CompletableFuture<>();
    }

    @Override
    public void cancel() {}
  }
}
