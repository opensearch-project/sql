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
import java.util.ArrayList;
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
    QueryJob retained = retainedJob(store, "ctx-retained");
    ResourceLimitExceededException e =
        assertThrows(
            ResourceLimitExceededException.class, () -> store.register(newJob("ctx-second")));
    assertTrue(e.getMessage().contains("plugins.query.async.max_retained_jobs"), e.getMessage());
    assertTrue(store.remove(retained.id(), retained));
    assertEquals(null, store.register(newJob("ctx-second")));
  }

  @Test
  void register_readsTheLimitOnEveryCall() {
    AtomicInteger limit = new AtomicInteger(2);
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(limit::get);
    retainedJob(store, "ctx-1");
    limit.set(1);
    assertThrows(ResourceLimitExceededException.class, () -> store.register(newJob("ctx-2")));
  }

  @Test
  void registeredJob_isNotCountedUntilRetained() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    QueryJob job = newJob("ctx-registered");
    store.register(job);
    assertTrue(admitsAnotherJob(store));
    store.retain(job);
    assertFalse(admitsAnotherJob(store));
  }

  @Test
  void retain_refusesAJobBeyondTheLimitAndLeavesItRegistered() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    QueryJob first = newJob("ctx-first");
    QueryJob second = newJob("ctx-second");
    store.register(first);
    store.register(second);
    store.retain(first);
    assertThrows(ResourceLimitExceededException.class, () -> store.retain(second));
    assertSame(second, store.find(second.id()).orElseThrow());
    assertTrue(store.remove(second.id(), second));
    assertFalse(admitsAnotherJob(store));
  }

  @Test
  void retain_isIdempotent() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    QueryJob job = retainedJob(store, "ctx-twice");
    store.retain(job);
    assertTrue(store.remove(job.id(), job));
    assertTrue(admitsAnotherJob(store));
  }

  @Test
  void retain_ignoresAJobThatIsNotRegistered() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    QueryJob job = newJob("ctx-unregistered");
    store.retain(job);
    assertTrue(admitsAnotherJob(store));
  }

  @Test
  void retainedJob_staysCountedAfterItFinishesUntilRemoved() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1);
    CompletableFuture<QueryResult> result = new CompletableFuture<>();
    QueryJob job = startedJob(store, "ctx-finishing", result);
    store.retain(job);
    assertFalse(admitsAnotherJob(store));
    result.complete(RESULT);
    assertFalse(admitsAnotherJob(store));
    assertTrue(store.remove(job.id(), job));
    assertTrue(admitsAnotherJob(store));
  }

  @Test
  void concurrentRetains_neverExceedTheLimit() throws Exception {
    int limit = 5;
    int submits = 50;
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> limit);
    List<QueryJob> registered = new ArrayList<>();
    for (int i = 0; i < submits; i++) {
      QueryJob job = newJob("ctx-" + i);
      store.register(job);
      registered.add(job);
    }
    ExecutorService pool = Executors.newFixedThreadPool(8);
    AtomicInteger accepted = new AtomicInteger();
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> retains = new ArrayList<>();
      for (QueryJob job : registered) {
        retains.add(
            pool.submit(
                () ->
                    await(
                        start,
                        () -> {
                          try {
                            store.retain(job);
                            accepted.incrementAndGet();
                          } catch (ResourceLimitExceededException e) {
                            // Refused: the limit is reached.
                          }
                        })));
      }
      start.countDown();
      for (Future<?> retain : retains) {
        retain.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(limit, accepted.get());
  }

  @Test
  void retainedJobsRemovedConcurrently_areReleasedExactlyOnce() throws Exception {
    AtomicInteger limit = new AtomicInteger(Integer.MAX_VALUE);
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(limit::get);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 2000; i++) {
        QueryJob job = retainedJob(store, "ctx-" + i);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> first = pool.submit(() -> await(start, () -> store.remove(job.id(), job)));
        Future<?> second = pool.submit(() -> await(start, () -> store.remove(job.id(), job)));
        start.countDown();
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    limit.set(1);
    // Nothing stays counted after every job was removed...
    assertTrue(admitsAnotherJob(store));
    // ...and nothing was released twice, so one retained job still fills a limit of one.
    retainedJob(store, "ctx-last");
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

  private static QueryJob retainedJob(InMemoryQueryJobStore store, String contextId) {
    QueryJob job = newJob(contextId);
    assertEquals(null, store.register(job));
    store.retain(job);
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
