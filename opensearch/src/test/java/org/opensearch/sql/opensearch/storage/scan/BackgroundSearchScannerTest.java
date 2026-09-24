/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.response.ShardStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

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
    request = mock(OpenSearchQueryRequest.class);
    executor = Executors.newSingleThreadExecutor();

    when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(threadPool.executor(any())).thenReturn(executor);

    scanner = new BackgroundSearchScanner(client, 10, 10);
  }

  @AfterEach
  void tearDown() {
    // Warnings live in a thread-local drained by the execution engine; tests share this thread.
    CalcitePlanContext.drainWarnings();
    executor.shutdownNow();
  }

  @Test
  void testSyncFallbackWhenNoNodeClient() {
    // Setup client without node client
    OpenSearchClient syncClient = mock(OpenSearchClient.class);
    when(syncClient.getNodeClient()).thenReturn(Optional.empty());
    scanner = new BackgroundSearchScanner(syncClient, 10, 10);

    OpenSearchResponse response = mockResponse(false, false, 10);
    when(syncClient.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

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
    BackgroundSearchScanner.SearchBatchResult result1 = scanner.fetchNextBatch(request);
    assertFalse(
        result1.stopIteration(), "Expected iteration to continue after fetching 10/15 results");
    assertTrue(result1.iterator().hasNext());

    // Second batch
    BackgroundSearchScanner.SearchBatchResult result2 = scanner.fetchNextBatch(request);
    assertTrue(result2.stopIteration());
    assertFalse(result2.iterator().hasNext());
  }

  @Test
  void testFetchOnceForAggregationResponse() {
    OpenSearchResponse response = mockResponse(false, true, 1);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
  }

  @Test
  void testFetchOnceWhenResultsBelowWindow() {
    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
  }

  @Test
  void raisesAWarningWhenAFetchedPageDidNotCoverEveryShard() {
    ShardStats partial = new ShardStats(4, 3, 0, 1, false, List.of("[logs][0] boom"));
    OpenSearchResponse response = mockResponse(false, true, 1, partial);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);

    List<Warning> warnings = CalcitePlanContext.drainWarnings();
    assertEquals(1, warnings.size());
    assertEquals(Warning.TYPE_PARTIAL_RESULT_SHARD_FAILURE, warnings.getFirst().getType());
    assertEquals("Results are partial: 1 of 4 shards failed.", warnings.getFirst().getMessage());
  }

  @Test
  void raisesNoWarningWhenEveryShardAnswered() {
    ShardStats complete = new ShardStats(4, 4, 0, 0, false, List.of());
    OpenSearchResponse response = mockResponse(false, true, 1, complete);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);

    assertTrue(CalcitePlanContext.drainWarnings().isEmpty());
  }

  @Test
  void collapsesTheSameShardOutcomeRepeatedAcrossPages() {
    // A paginated scan sees the same shard topology on every page; the user should be told once.
    ShardStats partial = new ShardStats(4, 3, 0, 1, false, List.of("[logs][0] boom"));
    OpenSearchResponse page1 = mockResponse(false, false, 10, partial);
    OpenSearchResponse page2 = mockResponse(false, false, 10, partial);
    OpenSearchResponse exhausted = mockResponse(true, false, 0, partial);
    when(client.search(request)).thenReturn(page1).thenReturn(page2).thenReturn(exhausted);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);
    scanner.fetchNextBatch(request);
    scanner.fetchNextBatch(request);

    assertEquals(1, CalcitePlanContext.drainWarnings().size());
  }

  @Test
  void testReset() {
    OpenSearchResponse response1 = mockResponse(false, false, 5);
    OpenSearchResponse response2 = mockResponse(true, false, 0);

    when(client.search(request)).thenReturn(response1).thenReturn(response2);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);
    scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());

    scanner.reset(request);

    assertFalse(scanner.isScanDone());
  }

  private OpenSearchResponse mockResponse(boolean isEmpty, boolean isAggregation, int numResults) {
    return mockResponse(isEmpty, isAggregation, numResults, ShardStats.UNKNOWN);
  }

  private OpenSearchResponse mockResponse(
      boolean isEmpty, boolean isAggregation, int numResults, ShardStats shardStats) {
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.isEmpty()).thenReturn(isEmpty);
    when(response.isAggregationResponse()).thenReturn(isAggregation);
    when(response.getShardStats()).thenReturn(shardStats);

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
