/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * Supports a simple iteration over a collection for OpenSearch index
 *
 * <p>Analogous to LINQ's System.Collections.Enumerator. Unlike LINQ, if the underlying collection
 * has been modified it is only optional that an implementation of the Enumerator interface detects
 * it and throws a {@link java.util.ConcurrentModificationException}.
 */
public class OpenSearchIndexEnumerator implements Enumerator<Object> {

  /** OpenSearch client. */
  private final OpenSearchClient client;

  private final List<String> fields;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResponseSize;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResultWindow;

  /** How many moveNext() calls to perform resource check once. */
  private static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /** ResourceMonitor. */
  private final ResourceMonitor monitor;

  /** Number of rows returned. */
  private Integer queryCount;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  private ExprValue current;

  private CompletableFuture<OpenSearchResponse> nextBatchFuture;
  private boolean isLastBatch = false;

  /** flag to indicate whether fetch more than one batch */
  private boolean fetchOnce = false;

  public OpenSearchIndexEnumerator(
      OpenSearchClient client,
      List<String> fields,
      int maxResponseSize,
      int maxResultWindow,
      OpenSearchRequest request,
      ResourceMonitor monitor) {
    this.client = client;
    this.fields = fields;
    this.request = request;
    this.maxResponseSize = maxResponseSize;
    this.maxResultWindow = maxResultWindow;
    this.monitor = monitor;
    this.queryCount = 0;
    this.current = null;
    if (!this.monitor.isHealthy()) {
      throw new NonFallbackCalciteException("insufficient resources to run the query, quit.");
    }
    this.nextBatchFuture = CompletableFuture.supplyAsync(() -> client.search(request));
  }

  private void fetchNextBatch() {
    try {
      OpenSearchResponse response = nextBatchFuture.get();
      // Start by determining whether we actually need future batches
      if (response.isAggregationResponse()
          || response.isCountResponse()
          || response.getHitsSize() < maxResultWindow) {
        // No need to fetch next batch if it's for an aggregation
        // or the length of response hits is less than max result window size.
        fetchOnce = true;
      }
      if (!response.isEmpty()) {
        iterator = response.iterator();

        // If we haven't hit the end, start pre-fetching next batch
        if (!isLastBatch && !fetchOnce) {
          nextBatchFuture = CompletableFuture.supplyAsync(() -> client.search(request));
        }
      } else {
        if (iterator == null) {
          iterator = Collections.emptyIterator();
        }
        isLastBatch = true;
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new NonFallbackCalciteException("Error fetching batch: " + e.getMessage());
    }
  }

  @Override
  public Object current() {
    /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
     * See {@link PhysTypeImpl}
     */
    if (fields.size() == 1) {
      return resolveForCalcite(current, fields.getFirst());
    }
    return fields.stream().map(field -> resolveForCalcite(current, field)).toArray();
  }

  private Object resolveForCalcite(ExprValue value, String rawPath) {
    return ExprValueUtils.resolveRefPaths(value, List.of(rawPath.split("\\."))).valueForCalcite();
  }

  @Override
  public boolean moveNext() {
    if (queryCount >= maxResponseSize) {
      isLastBatch = true;
      return false;
    }

    boolean shouldCheck = (queryCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck && !this.monitor.isHealthy()) {
      throw new NonFallbackCalciteException("insufficient resources to load next row, quit.");
    }

    if (iterator == null || (!iterator.hasNext() && !fetchOnce)) {
      fetchNextBatch();
    }
    if (iterator.hasNext()) {
      current = iterator.next();
      queryCount++;
      return true;
    } else {
      isLastBatch = true;
      return false;
    }
  }

  @Override
  public void reset() {
    isLastBatch = false;
    nextBatchFuture = CompletableFuture.supplyAsync(() -> client.search(request));
    OpenSearchResponse response = client.search(request);
    if (!response.isEmpty()) {
      iterator = response.iterator();
    } else {
      iterator = Collections.emptyIterator();
      isLastBatch = true;
    }
    queryCount = 0;
  }

  @Override
  public void close() {
    iterator = Collections.emptyIterator();
    queryCount = 0;
    isLastBatch = true;
    if (nextBatchFuture != null) {
      nextBatchFuture.cancel(true);
    }
    if (request != null) {
      client.forceCleanup(request);
      request = null;
    }
  }
}
