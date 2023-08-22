/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * OpenSearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchQueryRequest implements OpenSearchRequest {

  /** {@link OpenSearchRequest.IndexName}. */
  private final IndexName indexName;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder;

  /** OpenSearchExprValueFactory. */
  @EqualsAndHashCode.Exclude @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /** List of includes expected in the response. */
  @EqualsAndHashCode.Exclude @ToString.Exclude private final List<String> includes;

  /** Indicate the search already done. */
  private boolean searchDone = false;

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      String indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    this(new IndexName(indexName), size, factory, includes);
  }

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      IndexName indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
    this.exprValueFactory = factory;
    this.includes = includes;
  }

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = factory;
    this.includes = includes;
  }

  @Override
  public OpenSearchResponse search(
      Function<SearchRequest, SearchResponse> searchAction,
      Function<SearchScrollRequest, SearchResponse> scrollAction) {
    if (searchDone) {
      return new OpenSearchResponse(SearchHits.empty(), exprValueFactory, includes);
    } else {
      searchDone = true;
      return new OpenSearchResponse(
          searchAction.apply(
              new SearchRequest().indices(indexName.getIndexNames()).source(sourceBuilder)),
          exprValueFactory,
          includes);
    }
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    // do nothing.
  }

  @Override
  public boolean hasAnotherBatch() {
    return false;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    throw new UnsupportedOperationException(
        "OpenSearchQueryRequest serialization " + "is not implemented.");
  }
}
