/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.sql.expression.HighlightExpression.HIGHLIGHT_FIELD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;

/**
 * Supports a simple iteration over a collection for OpenSearch index
 *
 * <p>Analogous to LINQ's System.Collections.Enumerator. Unlike LINQ, if the underlying collection
 * has been modified it is only optional that an implementation of the Enumerator interface detects
 * it and throws a {@link java.util.ConcurrentModificationException}.
 */
public class OpenSearchIndexEnumerator implements Enumerator<Object> {

  /**
   * Thread-local collector for highlight data. Since the Calcite row pipeline only carries schema
   * column values, highlight metadata from OpenSearch hits is collected here as a side channel.
   * After execution, {@link #getAndClearCollectedHighlights()} retrieves the collected data so it
   * can be merged back into the ExprTupleValues for the JDBC response.
   */
  private static final ThreadLocal<List<ExprValue>> COLLECTED_HIGHLIGHTS =
      ThreadLocal.withInitial(ArrayList::new);

  /** Retrieve collected highlights and clear the ThreadLocal. */
  public static List<ExprValue> getAndClearCollectedHighlights() {
    List<ExprValue> result = new ArrayList<>(COLLECTED_HIGHLIGHTS.get());
    COLLECTED_HIGHLIGHTS.get().clear();
    return result;
  }

  /** Clear collected highlights (call before starting a new execution). */
  public static void clearCollectedHighlights() {
    COLLECTED_HIGHLIGHTS.get().clear();
  }

  /** OpenSearch client. */
  private final OpenSearchClient client;

  private final BackgroundSearchScanner bgScanner;

  private final List<String> fields;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResponseSize;

  /** How many moveNext() calls to perform resource check once. */
  private static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /** ResourceMonitor. */
  private final ResourceMonitor monitor;

  /** Number of rows returned. */
  private Integer queryCount = 0;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  private ExprValue current = null;

  public OpenSearchIndexEnumerator(
      OpenSearchClient client,
      List<String> fields,
      int maxResponseSize,
      int maxResultWindow,
      int queryBucketSize,
      OpenSearchRequest request,
      ResourceMonitor monitor) {
    if (!monitor.isHealthy()) {
      throw new NonFallbackCalciteException("insufficient resources to run the query, quit.");
    }

    this.fields = fields;
    this.request = request;
    this.maxResponseSize = maxResponseSize;
    this.monitor = monitor;
    this.client = client;
    this.bgScanner = new BackgroundSearchScanner(client, maxResultWindow, queryBucketSize);
    this.bgScanner.startScanning(request);
  }

  private Iterator<ExprValue> fetchNextBatch() {
    BackgroundSearchScanner.SearchBatchResult result = bgScanner.fetchNextBatch(request);
    return result.iterator();
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
      return false;
    }

    boolean shouldCheck = (queryCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck && !this.monitor.isHealthy()) {
      throw new NonFallbackCalciteException("insufficient resources to load next row, quit.");
    }

    if (iterator == null || (!iterator.hasNext() && !this.bgScanner.isScanDone())) {
      iterator = fetchNextBatch();
    }
    if (iterator.hasNext()) {
      current = iterator.next();
      // Collect highlight data as a side channel for the JDBC response.
      // The Calcite row (from current()) only carries schema column values,
      // so _highlight must be preserved separately.
      Map<String, ExprValue> tuple = ExprValueUtils.getTupleValue(current);
      ExprValue hl = tuple.get(HIGHLIGHT_FIELD);
      COLLECTED_HIGHLIGHTS.get().add(hl != null && !hl.isMissing() ? hl : null);
      queryCount++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    bgScanner.reset(request);
    iterator = bgScanner.fetchNextBatch(request).iterator();
    queryCount = 0;
    COLLECTED_HIGHLIGHTS.get().clear();
  }

  @Override
  public void close() {
    iterator = Collections.emptyIterator();
    queryCount = 0;
    bgScanner.close();
    if (request != null) {
      client.forceCleanup(request);
      request = null;
    }
  }
}
