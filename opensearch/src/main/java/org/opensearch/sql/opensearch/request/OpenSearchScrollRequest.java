/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
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

/**
 * OpenSearch scroll search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation 2)
 * Maintain scroll ID between calls to client search method
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchScrollRequest implements OpenSearchRequest {

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(100L);

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final IndexName indexName;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Scroll id which is set after first request issued. Because ElasticsearchClient is shared by
   * multi-thread so this state has to be maintained here.
   */
  @Setter
  @Getter
  private String scrollId;

  private boolean needClean = false;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder;

  /** Constructor. */
  public OpenSearchScrollRequest(IndexName indexName, OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    this.exprValueFactory = exprValueFactory;
  }

  public OpenSearchScrollRequest(String indexName, OpenSearchExprValueFactory exprValueFactory) {
    this(new IndexName(indexName), exprValueFactory);
  }

  /** Constructor. */
  public OpenSearchScrollRequest(IndexName indexName,
                                 SearchSourceBuilder sourceBuilder,
                                 OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = exprValueFactory;
  }

  /** Constructor. */
  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    if (isScroll()) {
      openSearchResponse = scrollAction.apply(scrollRequest());
    } else {
      openSearchResponse = searchAction.apply(searchRequest());
    }

    var response = new OpenSearchResponse(openSearchResponse, exprValueFactory);
    if (!(needClean = response.isEmpty())) {
      setScrollId(openSearchResponse.getScrollId());
    }
    return response;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      // clean on the last page only, to prevent closing the scroll/cursor in the middle of paging.
      if (needClean && isScroll()) {
        cleanAction.accept(getScrollId());
        setScrollId(null);
      }
    } finally {
      reset();
    }
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

  /**
   * Is scroll started which means pages after first is being requested.
   *
   * @return true if scroll started
   */
  public boolean isScroll() {
    return scrollId != null;
  }

  /**
   * Generate OpenSearch scroll request by scroll id maintained.
   *
   * @return scroll request
   */
  public SearchScrollRequest scrollRequest() {
    Objects.requireNonNull(scrollId, "Scroll id cannot be null");
    return new SearchScrollRequest().scroll(DEFAULT_SCROLL_TIMEOUT).scrollId(scrollId);
  }

  /**
   * Reset internal state in case any stale data. However, ideally the same instance is not supposed
   * to be reused across different physical plan.
   */
  public void reset() {
    scrollId = null;
  }

  /**
   * Convert a scroll request to string that can be included in a cursor.
   * @return a string representing the scroll request.
   */
  @Override
  public String toCursor() {
    return scrollId;
  }
}
