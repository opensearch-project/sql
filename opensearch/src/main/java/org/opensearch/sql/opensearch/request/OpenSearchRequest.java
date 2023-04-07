/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * OpenSearch search request.
 */
public interface OpenSearchRequest {
  /**
   * Apply the search action or scroll action on request based on context.
   *
   * @param searchAction search action.
   * @param scrollAction scroll search action.
   * @return ElasticsearchResponse.
   */
  OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                            Function<SearchScrollRequest, SearchResponse> scrollAction);

  /**
   * Apply the cleanAction on request.
   *
   * @param cleanAction clean action.
   */
  void clean(Consumer<String> cleanAction);

  /**
   * Get the SearchSourceBuilder.
   *
   * @return SearchSourceBuilder.
   */
  SearchSourceBuilder getSourceBuilder();

  /**
   * Get the ElasticsearchExprValueFactory.
   * @return ElasticsearchExprValueFactory.
   */
  OpenSearchExprValueFactory getExprValueFactory();

  /**
   * OpenSearch Index Name.
   * Indices are seperated by ",".
   */
  @EqualsAndHashCode
  class IndexName {
    private static final String COMMA = ",";
    private static final String COLON = ":";

    private final String[] indexFullNames;
    private final String[] indexNames;

    /**
     * Constructor.
     * indexNames are indexFullNames without the "{cluster}:" prefix.
     */
    public IndexName(String indexName) {
      this.indexFullNames = indexName.split(COMMA);
      // Remove all "<cluster>:" prefix if they exist
      this.indexNames = Arrays.stream(indexFullNames)
              .map(name -> name.substring(name.indexOf(COLON) + 1))
              .toArray(String[]::new);
    }

    public String[] getIndexFullNames() {
      return indexFullNames;
    }

    public String[] getIndexNames() {
      return indexNames;
    }

    @Override
    public String toString() {
      return String.join(COMMA, indexFullNames);
    }
  }
}
