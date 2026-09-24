/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Collections;
import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.jetbrains.annotations.TestOnly;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch index scan operator. */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchIndexScan extends TableScanOperator {

  /** OpenSearch client. */
  private OpenSearchClient client;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private int maxResponseSize;

  /** Returns the search request. */
  public OpenSearchRequest getRequest() {
    return request;
  }

  /** Returns the maximum number of rows allowed in the response. */
  public int getMaxResponseSize() {
    return maxResponseSize;
  }

  /**
   * Marks that this scan's PIT is represented by a successfully encoded cursor. Once marked, {@link
   * #close()} preserves the PIT so the next page can resume from it.
   */
  public void markCursorSerialized() {
    cursorSerialized = true;
  }

  /** Number of rows returned. */
  private Integer queryCount;

  /**
   * Whether the cursor (including PIT) has been serialized for a subsequent page request. When
   * true, {@link #close()} must preserve the PIT because a future request will resume from it.
   */
  private boolean cursorSerialized = false;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  /** Creates index scan based on a provided OpenSearchRequestBuilder. */
  public OpenSearchIndexScan(
      OpenSearchClient client, int maxResponseSize, OpenSearchRequest request) {
    this.maxResponseSize = maxResponseSize;
    this.client = client;
    this.request = request;
  }

  @TestOnly
  public OpenSearchIndexScan(OpenSearchClient client, OpenSearchRequest request) {
    this(client, Integer.MAX_VALUE, request);
  }

  @Override
  public void open() {
    super.open();
    iterator = Collections.emptyIterator();
    queryCount = 0;
    fetchNextBatch();
  }

  @Override
  public boolean hasNext() {
    // Check for thread interruption to support query timeout
    if (Thread.currentThread().isInterrupted()) {
      throw new OpenSearchTimeoutException(new InterruptedException("Query execution interrupted"));
    }

    // For pagination and limit, we need to limit the return rows count to pageSize or limit size
    if (queryCount >= maxResponseSize) {
      return false;
    }

    if (!iterator.hasNext()) {
      fetchNextBatch();
    }
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    // Check for thread interruption to support query timeout
    if (Thread.currentThread().isInterrupted()) {
      throw new OpenSearchTimeoutException(new InterruptedException("Query execution interrupted"));
    }

    queryCount++;
    return iterator.next();
  }

  private void fetchNextBatch() {
    OpenSearchResponse response = client.search(request);
    if (!response.isEmpty()) {
      iterator = response.iterator();
    }
  }

  @Override
  public void close() {
    super.close();

    if (request.hasAnotherBatch() && cursorSerialized) {
      // PIT has been serialized into a cursor for the next page request.
      // Only clean up in-memory state; the PIT must survive for the next request.
      client.cleanup(request);
    } else {
      // No more pages, or query failed/aborted before cursor was serialized.
      // Force delete the PIT to prevent leaking.
      client.forceCleanup(request);
    }
  }

  /**
   * Force cleanup of server-side resources (PIT) regardless of pagination state. Used by {@link
   * org.opensearch.sql.planner.physical.CursorCloseOperator} when the client explicitly closes a
   * cursor mid-pagination.
   */
  @Override
  public void forceClose() {
    super.close();
    client.forceCleanup(request);
  }

  @Override
  public String explain() {
    return request.toString();
  }
}
