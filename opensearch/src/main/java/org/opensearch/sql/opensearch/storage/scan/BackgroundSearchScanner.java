/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_BACKGROUND_THREAD_POOL_NAME;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.tasks.CancellableTask;

/**
 * Utility class for asynchronously scanning an index. This lets us send background requests to the
 * index while we work on processing the previous batch.
 *
 * <h2>Lifecycle</h2>
 *
 * The typical usage pattern is:
 *
 * <pre>
 *   1. Create scanner: new BackgroundSearchScanner(client)
 *   2. Start initial scan: startScanning(request)
 *   3. Fetch batches in a loop: fetchNextBatch(request, maxWindow)
 *   4. Close scanner when done: close()
 * </pre>
 *
 * <h2>Async vs Sync Behavior</h2>
 *
 * The scanner attempts to operate asynchronously when possible to improve performance:
 *
 * <ul>
 *   <li>When async is available (client has thread pool access): - Next batch is pre-fetched while
 *       current batch is being processed - Reduces latency between batches
 *   <li>When async is not available (client lacks thread pool access): - Falls back to synchronous
 *       fetching - Each batch is fetched only when needed
 * </ul>
 *
 * <h2>Termination Conditions</h2>
 *
 * Scanning will stop when any of these conditions are met:
 *
 * <ul>
 *   <li>An empty response is received (lastBatch = true)
 *   <li>Response is an aggregation or count response (fetchOnce = true)
 *   <li>Response size is less than maxResultWindow (fetchOnce = true)
 * </ul>
 *
 * Note: This class should be explicitly closed when no longer needed to ensure proper resource
 * cleanup.
 */
public class BackgroundSearchScanner {
  /**
   * Task header linking a child DSL search to its originating SQL/PPL query. Must match {@code
   * QueryInsightsMarker.PARENT_HEADER} in the {@code plugin} module; duplicated as a literal
   * because {@code opensearch} must not depend on {@code plugin}. OpenSearch only copies this
   * header onto a child SearchTask from the thread that issues the search, and background scans run
   * on a pool thread without the coordinator's context, so it is re-applied per search (see {@link
   * #searchWithParentHeader}).
   */
  private static final String QUERY_INSIGHTS_PARENT_HEADER = "X-Query-Insights-Parent";

  private final OpenSearchClient client;
  @Nullable private final Executor backgroundExecutor;
  private CompletableFuture<OpenSearchResponse> nextBatchFuture = null;
  private boolean stopIteration = false;
  private final int maxResultWindow;
  private final int queryBucketSize;

  public BackgroundSearchScanner(
      OpenSearchClient client, int maxResultWindow, int queryBucketSize) {
    this.client = client;
    this.maxResultWindow = maxResultWindow;
    this.queryBucketSize = queryBucketSize;
    // We can only actually do the background operation if we have the ability to access the thread
    // pool. Otherwise, fallback to synchronous fetch.
    if (client.getNodeClient().isPresent()) {
      this.backgroundExecutor =
          client.getNodeClient().get().threadPool().executor(SQL_BACKGROUND_THREAD_POOL_NAME);
    } else {
      this.backgroundExecutor = null;
    }
  }

  private boolean isAsync() {
    return backgroundExecutor != null;
  }

  /**
   * @return Whether the search scanner has fetched all batches
   */
  public boolean isScanDone() {
    return stopIteration;
  }

  /**
   * Initiates the scanning process. If async operations are available, this will trigger the first
   * background fetch.
   *
   * @param request The OpenSearch request to execute
   */
  public void startScanning(OpenSearchRequest request) {
    if (isAsync()) {
      ProfileContext ctx = QueryProfiling.current();
      // Capture task + parent marker on the calling thread; the background pool thread does not
      // inherit them and re-establishes both around the search.
      CancellableTask task = OpenSearchQueryManager.getCancellableTask();
      String parentMarker = currentQueryInsightsParentHeader();
      nextBatchFuture =
          CompletableFuture.supplyAsync(
              () ->
                  QueryProfiling.withCurrentContext(
                      ctx, () -> searchWithTask(request, task, parentMarker)),
              backgroundExecutor);
    }
  }

  /**
   * Parent marker on the calling thread, or null for non-SQL/PPL queries or when recording is off.
   */
  @Nullable
  private String currentQueryInsightsParentHeader() {
    return OpenSearchQueryManager.getQueryInsightsParentMarker();
  }

  /** Binds {@code task} to this pool thread for the search, restoring the previous task after. */
  private OpenSearchResponse searchWithTask(
      OpenSearchRequest request, @Nullable CancellableTask task, @Nullable String parentMarker) {
    if (task == null) {
      return searchWithParentHeader(request, parentMarker);
    }
    CancellableTask previous = OpenSearchQueryManager.getCancellableTask();
    OpenSearchQueryManager.setCancellableTask(task);
    try {
      return searchWithParentHeader(request, parentMarker);
    } finally {
      if (previous != null) {
        OpenSearchQueryManager.setCancellableTask(previous);
      } else {
        OpenSearchQueryManager.clearCancellableTask();
      }
    }
  }

  /**
   * Re-applies the parent-marker header to this pool thread's context so OpenSearch stamps it onto
   * the child SearchTask. Uses {@link ThreadContext#newStoredContext} (snapshot + restore) rather
   * than {@code stashContext()}: stashing would drop the security user transient and run the search
   * unauthenticated. Falls back to a plain search when there is no marker or node client.
   */
  private OpenSearchResponse searchWithParentHeader(
      OpenSearchRequest request, @Nullable String parentMarker) {
    if (parentMarker == null || parentMarker.isEmpty() || client.getNodeClient().isEmpty()) {
      return client.search(request);
    }
    ThreadContext threadContext = client.getNodeClient().get().threadPool().getThreadContext();
    // Already set on this pool thread: leave it be, putHeader would throw on a duplicate.
    if (threadContext.getHeader(QUERY_INSIGHTS_PARENT_HEADER) != null) {
      return client.search(request);
    }
    try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(true)) {
      threadContext.putHeader(QUERY_INSIGHTS_PARENT_HEADER, parentMarker);
      return client.search(request);
    }
  }

  private OpenSearchResponse getCurrentResponse(OpenSearchRequest request) {
    if (isAsync()) {
      try {
        return nextBatchFuture.get();
      } catch (OpenSearchSecurityException e) {
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TaskCancelledException("The task is cancelled.");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof OpenSearchSecurityException) {
          throw (OpenSearchSecurityException) e.getCause();
        }
        if (e.getCause() instanceof OpenSearchException) {
          if (((OpenSearchException) e.getCause()).getRootCause()
              instanceof ArrayIndexOutOfBoundsException) {
            // It could cause by searching CompositeAggregator with the last afterKey
            // which is not exist in the index.
            // In this case, we can safely ignore this exception.
            return OpenSearchResponse.EMPTY;
          }
        }
        throw new NonFallbackCalciteException(
            "Failed to fetch data from the index: the background task failed or interrupted.\n"
                + "  Inner error: "
                + e.getMessage(),
            e);
      }
    } else {
      return client.search(request);
    }
  }

  /**
   * Fetches the next batch of results. If async is enabled and more batches are expected, this will
   * also trigger the next background fetch.
   *
   * @param request The OpenSearch request to execute
   * @return SearchBatchResult containing the current batch's iterator and completion status
   * @throws NonFallbackCalciteException if the background fetch fails or is interrupted
   */
  public SearchBatchResult fetchNextBatch(OpenSearchRequest request) {
    OpenSearchResponse response = getCurrentResponse(request);

    // Determine if we need future batches
    if (response.isCountResponse()) {
      stopIteration = true;
    } else if (response.isCompositeAggregationResponse()) {
      // For composite aggregations, if we get fewer buckets than requested, we're done
      stopIteration = response.getCompositeBucketSize() < queryBucketSize;
    } else if (response.isAggregationResponse()) {
      stopIteration = true;
    } else {
      // For regular search results, if we get fewer hits than maxResultWindow, we're done
      stopIteration = response.getHitsSize() < maxResultWindow;
    }

    Iterator<ExprValue> iterator;
    if (!response.isEmpty()) {
      iterator = response.iterator();

      // Pre-fetch next batch if needed
      if (!stopIteration && isAsync()) {
        CancellableTask task = OpenSearchQueryManager.getCancellableTask();
        String parentMarker = currentQueryInsightsParentHeader();
        nextBatchFuture =
            CompletableFuture.supplyAsync(
                () -> searchWithTask(request, task, parentMarker), backgroundExecutor);
      }
    } else {
      iterator = Collections.emptyIterator();
      stopIteration = true;
    }

    return new SearchBatchResult(iterator, stopIteration);
  }

  /**
   * Resets the scanner to its initial state, allowing a new scan to begin. This clears all
   * completion flags and initiates a new background fetch if async is enabled.
   *
   * @param request The OpenSearch request to execute
   */
  public void reset(OpenSearchRequest request) {
    stopIteration = false;
    startScanning(request);
  }

  /**
   * Releases resources associated with this scanner. Cancels any pending background fetches and
   * marks the scan as complete. The scanner cannot be reused after closing without calling reset().
   */
  public void close() {
    stopIteration = true;
    if (nextBatchFuture != null) {
      nextBatchFuture.cancel(true);
    }
  }

  public record SearchBatchResult(Iterator<ExprValue> iterator, boolean stopIteration) {}
}
