/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinuePageRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.PagedRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.TableScanOperator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchPagedIndexScan extends TableScanOperator implements SerializablePlan {
  private OpenSearchClient client;
  @Getter
  private PagedRequestBuilder requestBuilder;
  @EqualsAndHashCode.Include
  @ToString.Include
  private OpenSearchRequest request;
  private Iterator<ExprValue> iterator;
  private long totalHits = 0;

  public OpenSearchPagedIndexScan(OpenSearchClient client, PagedRequestBuilder requestBuilder) {
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

  /** Don't use, it is for deserialization needs only. */
  @Deprecated
  public OpenSearchPagedIndexScan() {
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    var engine = (OpenSearchStorageEngine) ((PlanSerializer.CursorDeserializationStream) in)
        .resolveObject("engine");
    var indexName = (String) in.readUTF();
    var scrollId = (String) in.readUTF();
    client = engine.getClient();
    var index = new OpenSearchIndex(client, engine.getSettings(), indexName);
    requestBuilder = new ContinuePageRequestBuilder(
        new OpenSearchRequest.IndexName(indexName),
        scrollId, engine.getSettings(),
        new OpenSearchExprValueFactory(index.getFieldOpenSearchTypes()));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (request.toCursor() == null || request.toCursor().isEmpty()) {
      throw new NoCursorException();
    }

    out.writeUTF(requestBuilder.getIndexName().toString());
    out.writeUTF(request.toCursor());
  }
}
