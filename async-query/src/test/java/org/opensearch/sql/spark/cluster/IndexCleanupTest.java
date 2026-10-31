/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.sql.opensearch.client.PluginClient;

@ExtendWith(MockitoExtension.class)
class IndexCleanupTest {

  private static final String INDEX = "query_execution_result*";
  private static final QueryBuilder QUERY = QueryBuilders.matchAllQuery();

  @Mock private PluginClient client;
  @Mock private ClusterService clusterService;

  private final AtomicReference<DeleteByQueryRequest> sent = new AtomicReference<>();
  private final AtomicReference<ActionListener<BulkByScrollResponse>> transportListener =
      new AtomicReference<>();
  private final AtomicReference<Long> deleted = new AtomicReference<>();
  private final AtomicReference<Exception> failure = new AtomicReference<>();

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    doAnswer(
            invocation -> {
              sent.set(invocation.getArgument(1));
              transportListener.set(invocation.getArgument(2));
              return null;
            })
        .when(client)
        .execute(eq(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any());

    new IndexCleanup(client, clusterService)
        .deleteDocsByQuery(INDEX, QUERY, ActionListener.wrap(deleted::set, failure::set));
  }

  @Test
  void sendsALenientRefreshingDeleteByQueryThroughThePluginClient() {
    DeleteByQueryRequest request = sent.get();
    assertArrayEquals(new String[] {INDEX}, request.indices());
    assertEquals(IndicesOptions.LENIENT_EXPAND_OPEN, request.indicesOptions());
    assertTrue(request.isRefresh());
    assertSame(QUERY, request.getSearchRequest().source().query());
  }

  @Test
  void reportsTheDeletedCount() {
    transportListener.get().onResponse(response(3, List.of(), List.of()));

    assertEquals(3L, deleted.get());
    assertNull(failure.get());
  }

  @Test
  void failsWhenADeleteInTheRequestWasRejected() {
    RuntimeException denied = new RuntimeException("no permissions for indices:data/write/delete");
    transportListener
        .get()
        .onResponse(
            response(
                1, List.of(new BulkItemResponse.Failure(INDEX, "id", denied)), List.of()));

    assertNull(deleted.get());
    assertInstanceOf(IllegalStateException.class, failure.get());
    assertSame(denied, failure.get().getCause());
  }

  @Test
  void ignoresADocWrittenAfterTheSearchFoundIt() {
    BulkItemResponse.Failure conflict =
        new BulkItemResponse.Failure(
            INDEX, "id", new RuntimeException("version conflict"), RestStatus.CONFLICT);
    transportListener.get().onResponse(response(2, List.of(conflict), List.of()));

    assertEquals(2L, deleted.get());
    assertNull(failure.get());
  }

  @Test
  void failsWhenASearchInTheRequestWasRejected() {
    RuntimeException denied = new RuntimeException("no permissions for indices:data/read/search");
    transportListener
        .get()
        .onResponse(response(0, List.of(), List.of(new ScrollableHitSource.SearchFailure(denied))));

    assertNull(deleted.get());
    assertSame(denied, failure.get().getCause());
  }

  @Test
  void forwardsATransportFailure() {
    RuntimeException boom = new RuntimeException("boom");
    transportListener.get().onFailure(boom);

    assertNull(deleted.get());
    assertSame(boom, failure.get());
  }

  private static BulkByScrollResponse response(
      long deletedDocs,
      List<BulkItemResponse.Failure> bulkFailures,
      List<ScrollableHitSource.SearchFailure> searchFailures) {
    // Lenient, so a variant that ignores the failure lists fails on an assertion, not on stubbing.
    BulkByScrollResponse response =
        mock(BulkByScrollResponse.class, withSettings().strictness(Strictness.LENIENT));
    when(response.getDeleted()).thenReturn(deletedDocs);
    when(response.getBulkFailures()).thenReturn(bulkFailures);
    when(response.getSearchFailures()).thenReturn(searchFailures);
    return response;
  }
}
