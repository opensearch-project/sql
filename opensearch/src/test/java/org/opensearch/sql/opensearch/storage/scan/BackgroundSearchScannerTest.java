/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

class BackgroundSearchScannerTest {
  private OpenSearchClient client;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private OpenSearchRequest request;
  private BackgroundSearchScanner scanner;
  private ExecutorService executor;
  private ThreadContext threadContext;

  private static final String PARENT_HEADER = "X-Query-Insights-Parent";

  @BeforeEach
  void setUp() {
    client = mock(OpenSearchClient.class);
    nodeClient = mock(NodeClient.class);
    threadPool = mock(ThreadPool.class);
    request = mock(OpenSearchQueryRequest.class);
    executor = Executors.newSingleThreadExecutor();
    threadContext = new ThreadContext(Settings.EMPTY);

    when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(threadPool.getThreadContext()).thenReturn(threadContext);
    when(threadPool.executor(any())).thenReturn(executor);

    scanner = new BackgroundSearchScanner(client, 10, 10);
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
    OpenSearchQueryManager.clearCancellableTask();
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

  @Test
  void testCloseCancelsPendingFetch() {
    // A never-completing search keeps the background future pending so close() has something to
    // cancel.
    when(client.search(request))
        .thenAnswer(
            invocation -> {
              Thread.sleep(60_000);
              return mockResponse(true, false, 0);
            });

    scanner.startScanning(request);
    scanner.close();

    assertTrue(scanner.isScanDone());
  }

  @Test
  void testParentHeaderStampedOnBackgroundSearch() {
    // Drains in one full page + one empty page (hence two fetchNextBatch calls).
    threadContext.putHeader(PARENT_HEADER, "PPL:node-1:42");
    OpenSearchResponse page1 = mockResponse(false, false, 10);
    OpenSearchResponse page2 = mockResponse(true, false, 0);
    when(client.search(request)).thenReturn(page1).thenReturn(page2);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);
    scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
    verify(client, times(2)).search(request);
  }

  @Test
  void testNoParentHeaderRunsPlainSearch() {
    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
    assertNull(threadContext.getHeader(PARENT_HEADER));
  }

  @Test
  void testParentHeaderAlreadySetIsLeftAlone() {
    // Header already present on the pool thread (same shared context): the search runs without a
    // duplicate putHeader (which would throw).
    threadContext.putHeader(PARENT_HEADER, "PPL:node-1:7");
    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
    assertEquals("PPL:node-1:7", threadContext.getHeader(PARENT_HEADER));
  }

  @Test
  void testSearchWithTaskRestoresPreviousTask() {
    CancellableTask previous = mock(CancellableTask.class);
    OpenSearchQueryManager.setCancellableTask(previous);

    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);

    assertEquals(previous, OpenSearchQueryManager.getCancellableTask());
  }

  @Test
  void testCurrentParentHeaderNullWhenNoNodeClient() {
    OpenSearchClient syncClient = mock(OpenSearchClient.class);
    when(syncClient.getNodeClient()).thenReturn(Optional.empty());
    BackgroundSearchScanner syncScanner = new BackgroundSearchScanner(syncClient, 10, 10);

    OpenSearchResponse response = mockResponse(false, false, 5);
    when(syncClient.search(request)).thenReturn(response);

    syncScanner.startScanning(request);
    syncScanner.fetchNextBatch(request);

    verify(syncClient, times(1)).search(request);
  }

  @Test
  void testSecurityExceptionFromBackgroundFetchPropagates() {
    when(client.search(request)).thenThrow(new OpenSearchSecurityException("denied"));

    scanner.startScanning(request);

    assertThrows(OpenSearchSecurityException.class, () -> scanner.fetchNextBatch(request));
  }

  @Test
  void testArrayIndexOutOfBoundsFromBackgroundFetchYieldsEmpty() {
    // Composite aggregation on the last afterKey surfaces as ArrayIndexOutOfBounds wrapped in an
    // OpenSearchException; the scanner swallows it and returns an empty response.
    OpenSearchException wrapped =
        new OpenSearchException("wrap", new ArrayIndexOutOfBoundsException("last afterKey"));
    when(client.search(request)).thenThrow(wrapped);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

    assertTrue(result.stopIteration());
    assertFalse(result.iterator().hasNext());
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
