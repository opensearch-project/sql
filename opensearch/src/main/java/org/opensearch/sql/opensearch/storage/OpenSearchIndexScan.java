/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * OpenSearch index scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchIndexScan extends TableScanOperator {

  /** OpenSearch client. */
  private final OpenSearchClient client;

  /** Search request builder. */
  @Getter
  private final OpenSearchRequestBuilder requestBuilder;

  /** Search request. */
  @EqualsAndHashCode.Include
  @Getter
  @ToString.Include
  private OpenSearchRequest request;

  /** Total query size. */
  @EqualsAndHashCode.Include
  @Getter
  @ToString.Include
  private Integer querySize;

  /** Number of rows returned. */
  private Integer queryCount;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  /**
   * Constructor.
   */
  public OpenSearchIndexScan(OpenSearchClient client,
                             Settings settings, String indexName,
                             OpenSearchExprValueFactory exprValueFactory) {
    this(client, settings, new OpenSearchRequest.IndexName(indexName), exprValueFactory);
  }

  /**
   * Constructor.
   */
  public OpenSearchIndexScan(OpenSearchClient client,
      Settings settings, OpenSearchRequest.IndexName indexName,
      OpenSearchExprValueFactory exprValueFactory) {
    this.client = client;
    this.requestBuilder = new OpenSearchRequestBuilder(indexName, settings,
        new SearchSourceBuilder(), exprValueFactory);
  }

  @Override
  public void open() {
    super.open();
    querySize = requestBuilder.getSourceBuilder().size();
    request = requestBuilder.build();
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
    return getRequestBuilder().build().toString();
  }
}
