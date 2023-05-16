/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * {@link OpenSearchReader} using scroll search. https://opensearch.org/docs/latest/api-reference/scroll/
 */
public class OpenSearchScrollReader extends OpenSearchReader {

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1L);

  private final FlintOptions options;

  private String scrollId = null;

  public OpenSearchScrollReader(RestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder, FlintOptions options) {
    super(client, new SearchRequest().indices(indexName).source(searchSourceBuilder.size(options.getScrollSize())));
    this.options = options;
  }

  /**
   * search.
   */
  SearchResponse search(SearchRequest request) throws IOException {
    if (Strings.isNullOrEmpty(scrollId)) {
      // add scroll timeout making the request as scroll search request.
      request.scroll(DEFAULT_SCROLL_TIMEOUT);
      SearchResponse response = client.search(request, RequestOptions.DEFAULT);
      scrollId = response.getScrollId();
      return response;
    } else {
      return client.scroll(new SearchScrollRequest().scroll(DEFAULT_SCROLL_TIMEOUT).scrollId(scrollId), RequestOptions.DEFAULT);
    }
  }

  /**
   * clean the scroll context.
   */
  void clean() throws IOException {
    if (Strings.isNullOrEmpty(scrollId)) {
      ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.addScrollId(scrollId);
      client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }
  }
}
