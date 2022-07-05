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
 * OpenSearch scroll cursor request.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchScrollCursorRequest implements OpenSearchRequest {

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Scroll id.
   */
  private String scrollId;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

  public OpenSearchScrollCursorRequest(String scrollId, OpenSearchExprValueFactory exprValueFactory) {
    this.scrollId = scrollId;
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    openSearchResponse = scrollAction.apply(scrollRequest());

    return new OpenSearchResponse(openSearchResponse, exprValueFactory);
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      cleanAction.accept(getScrollId());
    }
    finally {

    }
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
}
