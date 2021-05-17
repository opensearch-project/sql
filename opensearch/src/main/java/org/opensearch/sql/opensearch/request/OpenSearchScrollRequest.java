/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.request;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
@Getter
@ToString
public class OpenSearchScrollRequest implements OpenSearchRequest {

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /** Index name. */
  private final String indexName;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Scroll id which is set after first request issued. Because ElasticsearchClient is shared by
   * multi-thread so this state has to be maintained here.
   */
  @Setter private String scrollId;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();


  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    if (isScrollStarted()) {
      openSearchResponse = scrollAction.apply(scrollRequest());
    } else {
      openSearchResponse = searchAction.apply(searchRequest());
    }
    setScrollId(openSearchResponse.getScrollId());

    return new OpenSearchResponse(openSearchResponse, exprValueFactory);
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      if (isScrollStarted()) {
        cleanAction.accept(getScrollId());
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
        .indices(indexName)
        .scroll(DEFAULT_SCROLL_TIMEOUT)
        .source(sourceBuilder);
  }

  /**
   * Is scroll started which means pages after first is being requested.
   *
   * @return true if scroll started
   */
  public boolean isScrollStarted() {
    return (scrollId != null);
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
}
