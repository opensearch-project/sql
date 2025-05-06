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
import lombok.ToString;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch index scan operator. */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchIndexScan extends TableScanOperator implements SerializablePlan {

  /** OpenSearch client. */
  private OpenSearchClient client;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private int maxResponseSize;

  /** Number of rows returned. */
  private Integer queryCount;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  /** Creates index scan based on a provided OpenSearchRequestBuilder. */
  public OpenSearchIndexScan(
      OpenSearchClient client, int maxResponseSize, OpenSearchRequest request) {
    this.client = client;
    this.maxResponseSize = maxResponseSize;
    this.request = request;
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
    if (queryCount >= maxResponseSize) {
      iterator = Collections.emptyIterator();
    } else if (!iterator.hasNext()) {
      fetchNextBatch();
    }
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
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

    client.cleanup(request);
  }

  @Override
  public String explain() {
    return request.toString();
  }

  /**
   * No-args constructor.
   *
   * @deprecated Exists only to satisfy Java serialization API.
   */
  @Deprecated(since = "introduction")
  public OpenSearchIndexScan() {}

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    int reqSize = in.readInt();
    byte[] requestStream = new byte[reqSize];
    int read = 0;
    do {
      int currentRead = in.read(requestStream, read, reqSize - read);
      if (currentRead == -1) {
        throw new IOException();
      }
      read += currentRead;
    } while (read < reqSize);

    var engine =
        (OpenSearchStorageEngine)
            ((PlanSerializer.CursorDeserializationStream) in).resolveObject("engine");

    client = engine.getClient();
    try (BytesStreamInput bsi = new BytesStreamInput(requestStream)) {
      request = new OpenSearchQueryRequest(bsi, engine);
    }
    maxResponseSize = in.readInt();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (!request.hasAnotherBatch()) {
      throw new NoCursorException();
    }
    // request is not directly Serializable so..
    // 1. Serialize request to an opensearch byte stream.
    BytesStreamOutput reqOut = new BytesStreamOutput();
    request.writeTo(reqOut);
    reqOut.flush();

    // 2. Extract byte[] from the opensearch byte stream
    var reqAsBytes = reqOut.bytes().toBytesRef().bytes;

    // 3. Write out the byte[] to object output stream.
    out.writeInt(reqOut.size());
    out.write(reqAsBytes, 0, reqOut.size());

    out.writeInt(maxResponseSize);
  }
}
