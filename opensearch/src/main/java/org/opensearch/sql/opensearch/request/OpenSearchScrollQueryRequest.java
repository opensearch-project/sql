/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * OpenSearch scroll search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation 2)
 * Maintain scroll ID between calls to client search method
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchScrollQueryRequest implements OpenSearchRequest {

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * {@link IndexName}.
   */
  private final IndexName indexName;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder;

  public OpenSearchScrollQueryRequest(IndexName indexName, int size, OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    this.exprValueFactory = exprValueFactory;
  }

  public OpenSearchScrollQueryRequest(String indexName, int size, OpenSearchExprValueFactory exprValueFactory) {
    this(new IndexName(indexName), size, exprValueFactory);
  }

  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    openSearchResponse = searchAction.apply(searchRequest());

    return new OpenSearchResponse(openSearchResponse, exprValueFactory);
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    // do nothing
  }

  /**
   * Generate OpenSearch search request.
   *
   * @return search request
   */
  public SearchRequest searchRequest() {
    return new SearchRequest()
        .indices(indexName.getIndexNames())
        .scroll(DEFAULT_SCROLL_TIMEOUT)
        .source(sourceBuilder);
  }
}
