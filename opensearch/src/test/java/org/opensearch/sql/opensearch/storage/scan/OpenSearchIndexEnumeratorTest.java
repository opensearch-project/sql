/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
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
  void completesWhenClosedEarly() {
    OpenSearchResponse response = response(3);
    when(client.search(request, progress)).thenReturn(response);
    OpenSearchIndexEnumerator enumerator = enumerator(5);

    enumerator.close();
    verify(progress).complete();
  }

  private OpenSearchIndexEnumerator enumerator(int maxResponseSize) {
    return new OpenSearchIndexEnumerator(
        client, List.of("f"), maxResponseSize, 10, 10, request, monitor, progress);
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
