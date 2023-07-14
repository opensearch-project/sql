/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * OpenSearch search request.
 */
public interface OpenSearchRequest extends Writeable {
  /**
   * Default query timeout in minutes.
   */
  TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * Apply the search action or scroll action on request based on context.
   *
   * @param searchAction search action.
   * @param scrollAction scroll search action.
   * @return OpenSearchResponse.
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
   * Get the OpenSearchExprValueFactory.
   * @return OpenSearchExprValueFactory.
   */
  OpenSearchExprValueFactory getExprValueFactory();

  /**
   * Check if there is more data to get from OpenSearch.
   * @return True if calling {@ref OpenSearchClient.search} with this request will
   *        return non-empty response.
   */
  boolean hasAnotherBatch();

  /**
   * OpenSearch Index Name.
   * Indices are separated by ",".
   */
  @EqualsAndHashCode
  class IndexName implements Writeable {
    private static final String COMMA = ",";

    private final String[] indexNames;

    public IndexName(StreamInput si) throws IOException {
      indexNames = si.readStringArray();
    }

    public IndexName(String indexName) {
      this.indexNames = indexName.split(COMMA);
    }

    public String[] getIndexNames() {
      return indexNames;
    }

    @Override
    public String toString() {
      return String.join(COMMA, indexNames);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeStringArray(indexNames);
    }
  }
}
