/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.monitor.ResourceStatus;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.progress.SourceProgress;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.response.ShardStats;

class OpenSearchIndexEnumeratorTest {

  private OpenSearchClient client;
  private OpenSearchRequest request;
  private ResourceMonitor monitor;
  private SourceProgress progress;

  @BeforeEach
  void setUp() {
    client = mock(OpenSearchClient.class);
    when(client.getNodeClient()).thenReturn(Optional.empty());
    request = mock(OpenSearchQueryRequest.class);
    monitor = mock(ResourceMonitor.class);
    when(monitor.getStatus())
        .thenReturn(ResourceStatus.healthy(ResourceStatus.ResourceType.MEMORY));
    progress = mock(SourceProgress.class);
  }

  @Test
  void startsProgressWithRowLimitAndCompletesWhenExhausted() {
    OpenSearchResponse response = response(1);
    when(client.search(request, progress)).thenReturn(response);
    OpenSearchIndexEnumerator enumerator = enumerator(5);

    verify(progress).start(5);
    assertTrue(enumerator.moveNext());
    verify(progress, never()).complete();
    assertFalse(enumerator.moveNext());
    verify(progress).complete();
  }

  @Test
  void completesWhenRowLimitIsReached() {
    OpenSearchResponse response = response(3);
    when(client.search(request, progress)).thenReturn(response);
    OpenSearchIndexEnumerator enumerator = enumerator(1);

    assertTrue(enumerator.moveNext());
    assertFalse(enumerator.moveNext());
    verify(progress).complete();
  }

  @Test
  void closeDoesNotComplete() {
    OpenSearchResponse response = response(3);
    when(client.search(request, progress)).thenReturn(response);
    OpenSearchIndexEnumerator enumerator = enumerator(5);

    enumerator.close();
    verify(progress, never()).complete();
  }

  @Test
  void projectsEveryNestedChildWithCorrelatedFields() {
    ExprValue empty = ExprValueUtils.tupleValue(Map.of("id", "empty", "events", List.of()));
    ExprValue parent =
        ExprValueUtils.tupleValue(
            Map.of(
                "id",
                "doc2",
                "events",
                List.of(
                    Map.of("name", "db_query", "status", "error"),
                    Map.of("name", "http_call", "status", "ok"))));
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.getShardStats()).thenReturn(ShardStats.UNKNOWN);
    when(response.getHitsSize()).thenReturn(2);
    when(response.iterator()).thenReturn(List.of(empty, parent).iterator());
    when(client.search(request, progress)).thenReturn(response);
    OpenSearchIndexEnumerator enumerator =
        new OpenSearchIndexEnumerator(
            client,
            List.of("id", "events.name", "events.status"),
            "events",
            10,
            10,
            10,
            request,
            monitor,
            progress);

    assertTrue(enumerator.moveNext());
    assertArrayEquals(new Object[] {"doc2", "db_query", "error"}, (Object[]) enumerator.current());
    assertTrue(enumerator.moveNext());
    assertArrayEquals(new Object[] {"doc2", "http_call", "ok"}, (Object[]) enumerator.current());
    assertFalse(enumerator.moveNext());
  }

  @Test
  void limitsExpandedRows() {
    ExprValue parent =
        ExprValueUtils.tupleValue(
            Map.of("events", List.of(Map.of("name", "first"), Map.of("name", "second"))));
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.getShardStats()).thenReturn(ShardStats.UNKNOWN);
    when(response.getHitsSize()).thenReturn(1);
    when(response.iterator()).thenReturn(List.of(parent).iterator());
    when(client.search(request, progress)).thenReturn(response);
    OpenSearchIndexEnumerator enumerator =
        new OpenSearchIndexEnumerator(
            client, List.of("events.name"), "events", 1, 10, 10, request, monitor, progress);

    assertTrue(enumerator.moveNext());
    assertEquals("first", enumerator.current());
    assertFalse(enumerator.moveNext());
    verify(progress).complete();
  }

  private OpenSearchIndexEnumerator enumerator(int maxResponseSize) {
    return new OpenSearchIndexEnumerator(
        client, List.of("f"), null, maxResponseSize, 10, 10, request, monitor, progress);
  }

  private static OpenSearchResponse response(int rows) {
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.getShardStats()).thenReturn(ShardStats.UNKNOWN);
    when(response.getHitsSize()).thenReturn(rows);
    when(response.iterator())
        .thenReturn(Collections.nCopies(rows, mock(ExprValue.class)).iterator());
    return response;
  }
}
