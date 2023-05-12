/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * Scroll (cursor) request is used to page the search. This request is not configurable and has
 * no search query. It just handles paging through responses to the initial request.
 * It is used on second and next pagination (cursor) requests.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class ContinuePageRequest implements OpenSearchRequest {
  private final String initialScrollId;
  private final TimeValue scrollTimeout;
  // ScrollId that OpenSearch returns after search.
  private String responseScrollId;

  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  @Getter
  private final OpenSearchExprValueFactory exprValueFactory;

  @EqualsAndHashCode.Exclude
  private boolean scrollFinished = false;

  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse = scrollAction.apply(new SearchScrollRequest(initialScrollId)
        .scroll(scrollTimeout));

    // TODO if terminated_early - something went wrong, e.g. no scroll returned.
    var response = new OpenSearchResponse(openSearchResponse, exprValueFactory, List.of());
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
  public void writeExternal(ObjectOutput out) {
    throw new UnsupportedOperationException("ContinuePageRequest.writeExternal");
  }

  @Override
  public void readExternal(ObjectInput in) {
    throw new UnsupportedOperationException("ContinuePageRequest.readExternal");
  }
}
