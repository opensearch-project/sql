/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.client.node.NodeClient;

class BackgroundSearchScannerTest {
  private OpenSearchClient client;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private OpenSearchRequest request;
  private BackgroundSearchScanner scanner;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    client = mock(OpenSearchClient.class);
    nodeClient = mock(NodeClient.class);
    threadPool = mock(ThreadPool.class);
    request = mock(OpenSearchRequest.class);
    executor = Executors.newSingleThreadExecutor();

    when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(threadPool.executor(any())).thenReturn(executor);

    scanner = new BackgroundSearchScanner(client);
  }

  @Test
  void testSyncFallbackWhenNoNodeClient() {
    // Setup client without node client
    OpenSearchClient syncClient = mock(OpenSearchClient.class);
    when(syncClient.getNodeClient()).thenReturn(Optional.empty());
    scanner = new BackgroundSearchScanner(syncClient);

    OpenSearchResponse response = mockResponse(false, false, 10);
    when(syncClient.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request, 10);

    assertFalse(
        result.stopIteration(), "Expected iteration to continue after fetching one full page");
    verify(syncClient, times(1)).search(request);
  }

  @Test
  void testCompleteScanWithMultipleBatches() {
    // First batch: normal response
    OpenSearchResponse response1 = mockResponse(false, false, 10);
    // Second batch: empty response
    OpenSearchResponse response2 = mockResponse(true, false, 5);

    when(client.search(request)).thenReturn(response1).thenReturn(response2);

    scanner.startScanning(request);

    // First batch
    BackgroundSearchScanner.SearchBatchResult result1 = scanner.fetchNextBatch(request, 10);
    assertFalse(
        result1.stopIteration(), "Expected iteration to continue after fetching 10/15 results");
    assertTrue(result1.iterator().hasNext());

    // Second batch
    BackgroundSearchScanner.SearchBatchResult result2 = scanner.fetchNextBatch(request, 10);
    assertTrue(result2.stopIteration());
    assertFalse(result2.iterator().hasNext());
  }

  @Test
  void testFetchOnceForAggregationResponse() {
    OpenSearchResponse response = mockResponse(false, true, 1);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request, 10);

    assertTrue(scanner.isScanDone());
  }

  @Test
  void testFetchOnceWhenResultsBelowWindow() {
    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request, 10);

    assertTrue(scanner.isScanDone());
  }

  @Test
  void testReset() {
    OpenSearchResponse response1 = mockResponse(false, false, 5);
    OpenSearchResponse response2 = mockResponse(true, false, 0);

    when(client.search(request)).thenReturn(response1).thenReturn(response2);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request, 10);
    scanner.fetchNextBatch(request, 10);

    assertTrue(scanner.isScanDone());

    scanner.reset(request);

    assertFalse(scanner.isScanDone());
  }

  private OpenSearchResponse mockResponse(boolean isEmpty, boolean isAggregation, int numResults) {
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.isEmpty()).thenReturn(isEmpty);
    when(response.isAggregationResponse()).thenReturn(isAggregation);

    if (numResults > 0) {
      ExprValue[] values = new ExprValue[numResults];
      Arrays.fill(values, mock(ExprValue.class));
      when(response.iterator()).thenReturn(Arrays.asList(values).iterator());
    } else {
      when(response.iterator()).thenReturn(Collections.emptyIterator());
    }

    when(response.getHitsSize()).thenReturn(numResults);
    return response;
  }
}
