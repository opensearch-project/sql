/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Collections;
import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

public class OpenSearchIndexEnumerator implements Enumerator<ExprValue> {

  /** OpenSearch client. */
  private final OpenSearchClient client;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private final OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResponseSize;

  /** Number of rows returned. */
  private Integer queryCount;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  public OpenSearchIndexEnumerator(
      OpenSearchClient client, int maxResponseSize, OpenSearchRequest request) {
    this.client = client;
    this.maxResponseSize = maxResponseSize;
    this.request = request;
    this.queryCount = 0;
    this.iterator = Collections.emptyIterator();
    fetchNextBatch();
  }

  private void fetchNextBatch() {
    OpenSearchResponse response = client.search(request);
    if (!response.isEmpty()) {
      iterator = response.iterator();
    }
  }

  @Override
  public ExprValue current() {
    queryCount++;
    return iterator.next();
  }

  @Override
  public boolean moveNext() {
    if (queryCount >= maxResponseSize) {
      iterator = Collections.emptyIterator();
    } else if (!iterator.hasNext()) {
      fetchNextBatch();
    }
    return iterator.hasNext();
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    client.cleanup(request);
  }
}
