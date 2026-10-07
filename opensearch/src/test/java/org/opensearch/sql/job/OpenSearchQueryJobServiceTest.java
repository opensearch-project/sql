/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;
import org.opensearch.threadpool.ThreadPool;

class OpenSearchQueryJobServiceTest {

  private static final Principal ALICE = new Principal("alice", null, List.of());
  private static final Principal BOB = new Principal("bob", null, List.of());
  private static final Duration WAIT = Duration.ofSeconds(30);
  private static final Duration KEEP_ALIVE = Duration.ofMinutes(5);
  private static final QueryResult.Rows RESULT =
      new QueryResult.Rows(
          new ExecutionEngine.Schema(List.of()), List.of(), Cursor.None, List.of(), 0);

  private InMemoryQueryJobStore store;
  private ThreadPool threadPool;
  private RetentionPolicy retentionPolicy;
  private OpenSearchQueryJobService service;

  @BeforeEach
  void setUp() {
    store = new InMemoryQueryJobStore();
    threadPool = mock(ThreadPool.class);
    retentionPolicy = new RetentionPolicy(store, threadPool);
    service = newService(retentionPolicy);
  }

  @Test
  void submit_startsRunnerAndReturnsRoutableIdWhenWaitExpires() {
    RecordingRunner runner = new RecordingRunner();
    QueryResult.Running running = submitRunning(runner);
    assertEquals("node-a", running.id().ownerNodeId());
    assertTrue(runner.wasRun());
    assertEquals(QueryJobState.RUNNING, service.get(running.id(), ALICE).state());
    verifyNoInteractions(threadPool);
  }

  @Test
  void submit_positiveWaitReturnsRunningWhenBudgetExpires() throws Exception {
    QueryResult result =
        service
            .submit(new RecordingRunner(), ALICE, Duration.ofMillis(1), KEEP_ALIVE)
            .toCompletableFuture()
            .get(1, TimeUnit.SECONDS);
    QueryResult.Running running = assertInstanceOf(QueryResult.Running.class, result);
    assertEquals(QueryJobState.RUNNING, service.get(running.id(), ALICE).state());
  }

  @Test
  void submit_inlineRowsAreRemovedBeforeResponseCallback() {
    RecordingRunner runner = new RecordingRunner();
    CompletionStage<Void> response =
        service
            .submit(runner, ALICE, WAIT, KEEP_ALIVE)
            .thenAccept(
                result -> {
                  assertSame(RESULT, result);
                  assertTrue(store.jobs().isEmpty());
                  verifyNoInteractions(threadPool);
                });

    runner.complete(RESULT);

    response.toCompletableFuture().join();
  }

  @Test
  void submit_alreadyCompletedRunnerReturnsInlineWithoutRetention() {
    RecordingRunner runner = new RecordingRunner();
    runner.complete(RESULT);

    QueryResult result =
        service.submit(runner, ALICE, WAIT, KEEP_ALIVE).toCompletableFuture().join();

    assertSame(RESULT, result);
    assertTrue(store.jobs().isEmpty());
    verifyNoInteractions(threadPool);
  }

  @Test
  void submit_inlineExplainIsRemovedWithoutRetention() {
    RecordingRunner runner = new RecordingRunner();
    QueryResult.Explain explain =
        new QueryResult.Explain(
            new ExecutionEngine.ExplainResponse(new ExecutionEngine.ExplainResponseNode("root")),
            0);
    CompletionStage<QueryResult> response = service.submit(runner, ALICE, WAIT, KEEP_ALIVE);

    runner.complete(explain);

    assertSame(explain, response.toCompletableFuture().join());
    assertTrue(store.jobs().isEmpty());
    verifyNoInteractions(threadPool);
  }

  @Test
  void submit_inlineFailureIsRemovedAndPreservesCause() {
    RecordingRunner runner = new RecordingRunner();
    IllegalArgumentException cause = new IllegalArgumentException("invalid query");
    CompletableFuture<QueryResult> response =
        service.submit(runner, ALICE, WAIT, KEEP_ALIVE).toCompletableFuture();

    runner.fail(cause);

    CompletionException failure = assertThrows(CompletionException.class, response::join);
    assertSame(cause, failure.getCause());
    assertTrue(store.jobs().isEmpty());
    verifyNoInteractions(threadPool);
  }

  @Test
  void submit_synchronousRunnerFailureIsRemovedAndPreservesCause() {
    QueryRunner runner = mock(QueryRunner.class);
    IllegalArgumentException cause = new IllegalArgumentException("preparation failed");
    when(runner.run()).thenThrow(cause);

    CompletableFuture<QueryResult> response =
        service.submit(runner, ALICE, WAIT, KEEP_ALIVE).toCompletableFuture();

    CompletionException failure = assertThrows(CompletionException.class, response::join);
    assertSame(cause, failure.getCause());
    assertTrue(store.jobs().isEmpty());
    verifyNoInteractions(threadPool);
  }

  @Test
  void submit_runningResultRetainsCompletionUntilTtl() {
    RecordingRunner runner = new RecordingRunner();
    QueryResult.Running running = submitRunning(runner);
    verifyNoInteractions(threadPool);

    runner.complete(RESULT);

    assertSame(RESULT, service.get(running.id(), ALICE).result().orElseThrow());
    expireRetainedJob();
    assertThrows(QueryJobNotFoundException.class, () -> service.get(running.id(), ALICE));
  }

  @Test
  void submit_completionBeforeRetentionRegistrationStillExpires() {
    RecordingRunner runner = new RecordingRunner();
    RetentionPolicy completingPolicy = spy(retentionPolicy);
    doAnswer(
            invocation -> {
              runner.complete(RESULT);
              return invocation.callRealMethod();
            })
        .when(completingPolicy)
        .arm(any(QueryJob.class), eq(KEEP_ALIVE));
    service = newService(completingPolicy);

    QueryResult.Running running = submitRunning(runner);

    assertSame(RESULT, service.get(running.id(), ALICE).result().orElseThrow());
    expireRetainedJob();
    assertThrows(QueryJobNotFoundException.class, () -> service.get(running.id(), ALICE));
  }

  @Test
  void submit_failureAfterRunningResultIsRetainedUntilTtl() {
    RecordingRunner runner = new RecordingRunner();
    QueryResult.Running running = submitRunning(runner);

    runner.fail(new IllegalStateException("execution failed"));

    assertEquals(QueryJobState.FAILED, service.get(running.id(), ALICE).state());
    expireRetainedJob();
    assertThrows(QueryJobNotFoundException.class, () -> service.get(running.id(), ALICE));
  }

  @Test
  void get_forbidsOtherPrincipal() {
    QueryResult.Running running = submitRunning(new RecordingRunner());
    assertThrows(QueryJobForbiddenException.class, () -> service.get(running.id(), BOB));
  }

  @Test
  void cancel_authorizesAndTransitionsRetainedJob() {
    RecordingRunner runner = new RecordingRunner();
    QueryResult.Running running = submitRunning(runner);

    QueryJobStatus status = service.cancel(running.id(), ALICE);

    assertEquals(QueryJobState.CANCELLED, status.state());
    assertTrue(runner.wasCancelled());
    expireRetainedJob();
    assertThrows(QueryJobNotFoundException.class, () -> service.get(running.id(), ALICE));
  }

  @Test
  void cancel_forbidsOtherPrincipal() {
    QueryResult.Running running = submitRunning(new RecordingRunner());
    assertThrows(QueryJobForbiddenException.class, () -> service.cancel(running.id(), BOB));
    assertEquals(QueryJobState.RUNNING, service.get(running.id(), ALICE).state());
  }

  @Test
  void get_throwsNotFoundForMissingId() {
    assertThrows(
        QueryJobNotFoundException.class,
        () -> service.get(new QueryJobId("node-a", "missing"), ALICE));
  }

  @Test
  void submit_mintsUniqueIdsPerCall() {
    QueryResult.Running first = submitRunning(new RecordingRunner());
    QueryResult.Running second = submitRunning(new RecordingRunner());
    assertNotEquals(first.id(), second.id());
  }

  @Test
  void submit_rejectsInvalidArgumentsBeforePublishing() {
    RecordingRunner runner = new RecordingRunner();
    assertThrows(NullPointerException.class, () -> service.submit(null, ALICE, WAIT, KEEP_ALIVE));
    assertThrows(NullPointerException.class, () -> service.submit(runner, null, WAIT, KEEP_ALIVE));
    assertThrows(NullPointerException.class, () -> service.submit(runner, ALICE, null, KEEP_ALIVE));
    assertThrows(NullPointerException.class, () -> service.submit(runner, ALICE, WAIT, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.submit(runner, ALICE, Duration.ofMillis(-1), KEEP_ALIVE));
    assertThrows(
        IllegalArgumentException.class, () -> service.submit(runner, ALICE, WAIT, Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.submit(runner, ALICE, WAIT, Duration.ofSeconds(-1)));
    assertFalse(runner.wasRun());
    assertTrue(store.jobs().isEmpty());
    verifyNoInteractions(threadPool);
  }

  private QueryResult.Running submitRunning(RecordingRunner runner) {
    return assertInstanceOf(
        QueryResult.Running.class,
        service.submit(runner, ALICE, Duration.ZERO, KEEP_ALIVE).toCompletableFuture().join());
  }

  private void expireRetainedJob() {
    ArgumentCaptor<Runnable> eviction = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool)
        .schedule(
            eviction.capture(),
            eq(TimeValue.timeValueMillis(KEEP_ALIVE.toMillis())),
            eq(ThreadPool.Names.GENERIC));
    eviction.getValue().run();
  }

  private OpenSearchQueryJobService newService(RetentionPolicy policy) {
    return newService(policy, null);
  }

  private OpenSearchQueryJobService newService(
      RetentionPolicy policy,
      java.util.function.Function<Throwable, java.util.Map<String, Object>> renderer) {
    ClusterService clusterService = mock(ClusterService.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("node-a");
    when(clusterService.localNode()).thenReturn(localNode);
    return new OpenSearchQueryJobService(
        store, clusterService, Clock.systemUTC(), policy, renderer);
  }

  @Test
  void submit_rendererCapturesStructuredDetailsOnFailure() throws Exception {
    service =
        newService(
            retentionPolicy,
            t -> java.util.Map.of("code", "FIELD_NOT_FOUND", "reason", t.getMessage()));
    RecordingRunner runner = new RecordingRunner();
    QueryResult result =
        service
            .submit(runner, ALICE, Duration.ofMillis(1), KEEP_ALIVE)
            .toCompletableFuture()
            .get(2, TimeUnit.SECONDS);
    QueryResult.Running running = assertInstanceOf(QueryResult.Running.class, result);
    runner.fail(new IllegalArgumentException("Field [x] not found."));
    QueryFailure failure = service.get(running.id(), ALICE).failure().orElseThrow();
    assertEquals("FIELD_NOT_FOUND", failure.details().get("code"));
    assertEquals("Field [x] not found.", failure.details().get("reason"));
  }

  private static final class RecordingRunner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private boolean ran;
    private boolean cancelled;

    @Override
    public CompletionStage<QueryResult> run() {
      ran = true;
      return future;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }

    boolean wasRun() {
      return ran;
    }

    boolean wasCancelled() {
      return cancelled;
    }

    void complete(QueryResult result) {
      future.complete(result);
    }

    void fail(Exception cause) {
      future.completeExceptionally(cause);
    }
  }
}
