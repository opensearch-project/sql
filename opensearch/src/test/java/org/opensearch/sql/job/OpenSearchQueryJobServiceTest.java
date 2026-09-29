/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
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

  @Test
  void submit_wrapsRunnerAndStartsIt() {
    RecordingRunner runner = new RecordingRunner();
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(runner, ALICE);
    assertEquals("node-a", job.id().ownerNodeId());
    assertTrue(runner.wasRun());
  }

  @Test
  void get_returnsStatusForOwner() {
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(new RecordingRunner(), ALICE);
    assertEquals(job.id(), service.get(job.id(), ALICE).id());
  }

  @Test
  void get_forbidsOtherPrincipal() {
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(new RecordingRunner(), ALICE);
    assertThrows(QueryJobForbiddenException.class, () -> service.get(job.id(), BOB));
  }

  @Test
  void cancel_authorizesAndTransitions() {
    OpenSearchQueryJobService service = newService();
    QueryJob job = service.submit(new RecordingRunner(), ALICE);
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
    QueryJob a = service.submit(new RecordingRunner(), ALICE);
    QueryJob b = service.submit(new RecordingRunner(), ALICE);
    assertNotEquals(a.id(), b.id());
  }

  @Test
  void submit_rejectsNullRunner() {
    OpenSearchQueryJobService service = newService();
    assertThrows(NullPointerException.class, () -> service.submit(null, ALICE));
  }

  private OpenSearchQueryJobService newService() {
    ClusterService clusterService = mock(ClusterService.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("node-a");
    when(clusterService.localNode()).thenReturn(localNode);
    return new OpenSearchQueryJobService(
        new InMemoryQueryJobStore(), clusterService, Clock.systemUTC(), null);
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
  }
}
