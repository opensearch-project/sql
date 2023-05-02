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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinuePageRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.PushDownRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * OpenSearch index scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchIndexScan extends TableScanOperator implements SerializablePlan {

  /** OpenSearch client. */
  private transient OpenSearchClient client;

  private transient OpenSearchRequest.IndexName indexName;
  private transient Settings settings;
  private transient Integer maxResultWindow;
  /** Search request builder. */
  @EqualsAndHashCode.Include
  @ToString.Include
  private transient PushDownRequestBuilder requestBuilder;

  /** Search request. */
  @EqualsAndHashCode.Include
  @ToString.Include
  private transient OpenSearchRequest request;

  /** Total query size. */
  @EqualsAndHashCode.Include
  @ToString.Include
  private Integer querySize;

  /** Number of rows returned. */
  private Integer queryCount;

  /** Search response for current batch. */
  private transient Iterator<ExprValue> iterator;

  /**
   * Factory method used in tests.
   */
  public static OpenSearchIndexScan create(OpenSearchClient client,
                                           String indexName,
                                           Settings settings,
                                           Integer maxResultWindow,
                                           OpenSearchExprValueFactory exprValueFactory) {
    final var name = new OpenSearchRequest.IndexName(indexName);
    final int defaultQuerySize = settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT);
    final var requestBuilder = new OpenSearchRequestBuilder(defaultQuerySize, exprValueFactory);
    return new OpenSearchIndexScan(client, name,
        settings, maxResultWindow, requestBuilder);
  }


  public OpenSearchIndexScan(OpenSearchClient client,
                             OpenSearchRequest.IndexName indexName,
                             Settings settings,
                             Integer maxResultWindow,
                             PushDownRequestBuilder requestBuilder) {
    this.client = client;
    this.indexName = indexName;
    this.settings = settings;
    this.maxResultWindow = maxResultWindow;
    this.requestBuilder = requestBuilder;
  }

  @Override
  public void open() {
    super.open();
    request = requestBuilder.build(indexName, maxResultWindow, settings);
    // TODO does this work for paged requests?
    querySize = requestBuilder.getQuerySize();
    iterator = Collections.emptyIterator();
    queryCount = 0;
    fetchNextBatch();
  }

  @Override
  public boolean hasNext() {
    if (queryCount >= querySize) {
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

  @Override
  public long getTotalHits() {
    // ignore response.getTotalHits(), because response returns entire index, regardless of LIMIT
    return queryCount;
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
    return requestBuilder.build(indexName, maxResultWindow, settings).toString();
  }

  /**
   * @deprecated Exists only to satisfy Java serialization API.
   */
  @Deprecated(since="introduction")
  public OpenSearchIndexScan() {
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    var serializedName = in.readUTF();
    var scrollId = in.readUTF();
    querySize = in.readInt();

    var engine = (OpenSearchStorageEngine) ((PlanSerializer.CursorDeserializationStream) in)
        .resolveObject("engine");

    indexName = new OpenSearchRequest.IndexName(serializedName);
    client = engine.getClient();
    settings = engine.getSettings();
    var index = new OpenSearchIndex(client, settings, indexName.toString());
    maxResultWindow = index.getMaxResultWindow();

    TimeValue scrollTimeout = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    requestBuilder = new ContinuePageRequestBuilder(scrollId, scrollTimeout,
        new OpenSearchExprValueFactory(index.getFieldOpenSearchTypes()));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (request.toCursor() == null || request.toCursor().isEmpty()) {
      throw new NoCursorException();
    }

    out.writeUTF(indexName.toString());
    out.writeUTF(request.toCursor());
    out.writeInt(querySize);
  }
}
