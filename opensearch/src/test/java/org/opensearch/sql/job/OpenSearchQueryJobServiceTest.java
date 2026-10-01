/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;

class OpenSearchQueryJobServiceTest {

  private static final Principal ALICE = new Principal("alice", null, List.of());
  private static final Principal BOB = new Principal("bob", null, List.of());
  private static final Duration KEEP_ALIVE = Duration.ofMinutes(5);

  @Test
  void submit_wrapsRunnerAndStartsIt() {
    RecordingRunner runner = new RecordingRunner();
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(runner, ALICE, KEEP_ALIVE);
    assertEquals("node-a", job.id().ownerNodeId());
    assertTrue(runner.wasRun());
  }

  @Test
  void get_returnsStatusForOwner() {
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(new RecordingRunner(), ALICE, KEEP_ALIVE);
    assertEquals(job.id(), service.get(job.id(), ALICE).id());
  }

  @Test
  void get_forbidsOtherPrincipal() {
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(new RecordingRunner(), ALICE, KEEP_ALIVE);
    assertThrows(QueryJobForbiddenException.class, () -> service.get(job.id(), BOB));
  }

  @Test
  void cancel_authorizesAndTransitions() {
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(new RecordingRunner(), ALICE, KEEP_ALIVE);
    QueryJobStatus status = service.cancel(job.id(), ALICE);
    assertEquals(QueryJobState.CANCELLED, status.state());
  }

  @Test
  void get_throwsNotFoundForMissingId() {
    OpenSearchQueryJobService service = newService();
    assertThrows(
        QueryJobNotFoundException.class,
        () -> service.get(new QueryJobId("node-a", "missing"), ALICE));
  }

  @Test
  void submit_mintsUniqueIdsPerCall() {
    OpenSearchQueryJobService service = newService();
    QueryJob a = service.submit(new RecordingRunner(), ALICE, KEEP_ALIVE);
    QueryJob b = service.submit(new RecordingRunner(), ALICE, KEEP_ALIVE);
    assertNotEquals(a.id(), b.id());
  }

  @Test
  void submit_rejectsNullRunner() {
    OpenSearchQueryJobService service = newService();
    assertThrows(NullPointerException.class, () -> service.submit(null, ALICE, KEEP_ALIVE));
  }

  @Test
  void submit_rejectsNullKeepAlive() {
    OpenSearchQueryJobService service = newService();
    assertThrows(
        NullPointerException.class, () -> service.submit(new RecordingRunner(), ALICE, null));
  }

  @Test
  void discard_removesJobFromStore() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    OpenSearchQueryJobService service = newService(store);
    QueryJob job = service.submit(new RecordingRunner(), ALICE, KEEP_ALIVE);
    assertTrue(store.find(job.id()).isPresent());

    service.discard(job);

    assertFalse(store.find(job.id()).isPresent());
    // Idempotent — a later retention eviction (conditional remove) is a no-op.
    service.discard(job);
    assertFalse(store.find(job.id()).isPresent());
  }

  @Test
  void discard_cancelsRetentionTimerSoSchedulerDoesNotRetainJob() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    org.opensearch.threadpool.ThreadPool threadPool =
        mock(org.opensearch.threadpool.ThreadPool.class);
    org.opensearch.threadpool.Scheduler.ScheduledCancellable cancellable =
        mock(org.opensearch.threadpool.Scheduler.ScheduledCancellable.class);
    // Pretend the scheduler queued the task (and would hold the job for the full keep_alive).
    // Capture the Runnable argument but do not execute it — simulate real deferred eviction.
    when(threadPool.schedule(any(Runnable.class), any(), anyString())).thenReturn(cancellable);
    RetentionPolicy retention = new RetentionPolicy(store, threadPool);

    ClusterService clusterService = mock(ClusterService.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("node-a");
    when(clusterService.localNode()).thenReturn(localNode);
    OpenSearchQueryJobService service =
        new OpenSearchQueryJobService(store, clusterService, Clock.systemUTC(), retention);

    RecordingRunner runner = new RecordingRunner();
    QueryJob job = service.submit(runner, ALICE, KEEP_ALIVE);
    // Drive to terminal so retention schedules the eviction task.
    runner.complete();

    service.discard(job);

    // disarm must have cancelled the scheduler's queued task so the captured job is reclaimable.
    verify(cancellable).cancel();
    assertFalse(store.find(job.id()).isPresent());
  }

  @Test
  void submit_rejectsNonPositiveKeepAlive() {
    OpenSearchQueryJobService service = newService();
    assertThrows(
        IllegalArgumentException.class,
        () -> service.submit(new RecordingRunner(), ALICE, Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.submit(new RecordingRunner(), ALICE, Duration.ofSeconds(-1)));
  }

  private OpenSearchQueryJobService newService() {
    return newService(new InMemoryQueryJobStore());
  }

  private OpenSearchQueryJobService newService(InMemoryQueryJobStore store) {
    ClusterService clusterService = mock(ClusterService.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("node-a");
    when(clusterService.localNode()).thenReturn(localNode);
    return new OpenSearchQueryJobService(store, clusterService, Clock.systemUTC(), null);
  }

  private static final class RecordingRunner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private boolean ran;

    @Override
    public CompletionStage<QueryResult> run() {
      ran = true;
      return future;
    }

    @Override
    public void cancel() {}

    boolean wasRun() {
      return ran;
    }

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
