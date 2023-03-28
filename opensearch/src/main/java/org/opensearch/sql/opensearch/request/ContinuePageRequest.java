/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.sql.opensearch.request.OpenSearchScrollRequest.DEFAULT_SCROLL_TIMEOUT;

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * Scroll (cursor) request is used to page the search. This request is not configurable and has
 * no search query. It just handles paging through responses to the initial request.
 * It is used on second and next pagination (cursor) requests.
 * First (initial) request is handled by {@link InitialPageRequestBuilder}.
 */
@EqualsAndHashCode
public class ContinuePageRequest implements OpenSearchRequest {
  final String initialScrollId;

  // ScrollId that OpenSearch returns after search.
  String responseScrollId;

  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  @Getter
  private final OpenSearchExprValueFactory exprValueFactory;

  @EqualsAndHashCode.Exclude
  private boolean scrollFinished = false;

  public ContinuePageRequest(String scrollId, OpenSearchExprValueFactory exprValueFactory) {
    this.initialScrollId = scrollId;
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse = scrollAction.apply(new SearchScrollRequest(initialScrollId)
        .scroll(DEFAULT_SCROLL_TIMEOUT));

    // TODO if terminated_early - something went wrong, e.g. no scroll returned.
    var response = new OpenSearchResponse(openSearchResponse, exprValueFactory);
    // on the last empty page, we should close the scroll
    scrollFinished = response.isEmpty();
    responseScrollId = openSearchResponse.getScrollId();
    return response;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    if (scrollFinished) {
      cleanAction.accept(responseScrollId);
    }
  }

  @Override
  public SearchSourceBuilder getSourceBuilder() {
    throw new UnsupportedOperationException(
        "SearchSourceBuilder is unavailable for ContinueScrollRequest");
  }

  @Override
  public String toCursor() {
    // on the last page, we shouldn't return the scroll to user, it is kept for closing (clean)
    return scrollFinished ? null : responseScrollId;
  }
}
