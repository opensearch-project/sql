/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
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
    this.querySize = requestBuilder.getSourceBuilder().size();
    this.request = requestBuilder.build();

    // For now pull all results immediately once open
    List<OpenSearchResponse> responses = new ArrayList<>();
    OpenSearchResponse response = client.search(request);
    while (!response.isEmpty()) {
      responses.add(response);
      response = client.search(request);
    }
    iterator = Iterables.concat(responses.toArray(new OpenSearchResponse[0])).iterator();
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
  public void close() {
    super.close();

    client.cleanup(request);
  }

  @Override
  public String explain() {
    return getRequestBuilder().build().toString();
  }
}
