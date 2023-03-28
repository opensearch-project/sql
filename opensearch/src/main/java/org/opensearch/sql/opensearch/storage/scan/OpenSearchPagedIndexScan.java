/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Collections;
import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.PagedRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.storage.TableScanOperator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchPagedIndexScan extends TableScanOperator {
  private final OpenSearchClient client;
  private final PagedRequestBuilder requestBuilder;
  @EqualsAndHashCode.Include
  @ToString.Include
  private OpenSearchRequest request;
  private Iterator<ExprValue> iterator;
  private long totalHits = 0;

  public OpenSearchPagedIndexScan(OpenSearchClient client,
                                  PagedRequestBuilder requestBuilder) {
    this.client = client;
    this.requestBuilder = requestBuilder;
  }

  @Override
  public String explain() {
    throw new NotImplementedException("Implement OpenSearchPagedIndexScan.explain");
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public void open() {
    super.open();
    request = requestBuilder.build();
    OpenSearchResponse response = client.search(request);
    if (!response.isEmpty()) {
      iterator = response.iterator();
      totalHits = response.getTotalHits();
    } else {
      iterator = Collections.emptyIterator();
    }
  }

  @Override
  public void close() {
    super.close();
    client.cleanup(request);
  }

  @Override
  public long getTotalHits() {
    return totalHits;
  }

  @Override
  public String toCursor() {
    // TODO this assumes exactly one index is scanned.
    var indexName = requestBuilder.getIndexName().getIndexNames()[0];
    var cursor = request.toCursor();
    return cursor == null || cursor.isEmpty()
        ? "" : createSection("OpenSearchPagedIndexScan", indexName, cursor);
  }
}
