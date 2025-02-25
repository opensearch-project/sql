/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

public class OpenSearchIndexEnumerator implements Enumerator<Object> {

  /** OpenSearch client. */
  private final OpenSearchClient client;

  private final List<String> fields;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private final OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResponseSize;

  /** Number of rows returned. */
  private Integer queryCount;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  private ExprValue current;

  public OpenSearchIndexEnumerator(
      OpenSearchClient client,
      List<String> fields,
      int maxResponseSize,
      OpenSearchRequest request) {
    this.client = client;
    this.fields = fields;
    this.maxResponseSize = maxResponseSize;
    this.request = request;
    this.queryCount = 0;
    this.current = null;
  }

  private void fetchNextBatch() {
    OpenSearchResponse response = client.search(request);
    if (!response.isEmpty()) {
      iterator = response.iterator();
    } else if (iterator == null) {
      iterator = Collections.emptyIterator();
    }
  }

  @Override
  public Object current() {
    Object[] p = fields.stream().map(k -> current.tupleValue().get(k).valueForCalcite()).toArray();
    return p;
  }

  @Override
  public boolean moveNext() {
    if (queryCount >= maxResponseSize) {
      iterator = Collections.emptyIterator();
      return false;
    } else if (iterator == null || !iterator.hasNext()) {
      fetchNextBatch();
    }
    if (iterator.hasNext()) {
      current = iterator.next();
      queryCount++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    iterator = Collections.emptyIterator();
    queryCount = 0;
  }

  @Override
  public void close() {
    reset();
    client.cleanup(request);
  }
}
