/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.pit.PointInTimeHandler;
import org.opensearch.transport.client.Client;

/** Executor for search requests with pagination. */
public abstract class ElasticHitsExecutor {
  protected static final Logger LOG = LogManager.getLogger();
  protected PointInTimeHandler pit;
  protected Client client;

  /**
   * Executes search request
   *
   * @throws IOException If an input or output exception occurred
   * @throws SqlParseException If parsing exception occurred
   */
  protected abstract void run() throws IOException, SqlParseException;

  /**
   * Get search hits after execution
   *
   * @return Search hits
   */
  protected abstract SearchHits getHits();

  /**
   * Get response for search request with pit/scroll
   *
   * @param request search request
   * @param select sql select
   * @param size fetch size
   * @param previousResponse response for previous request
   * @param pit point in time
   * @return search response for subsequent request
   */
  public SearchResponse getResponseWithHits(
      SearchRequestBuilder request,
      Select select,
      int size,
      SearchResponse previousResponse,
      PointInTimeHandler pit) {
    // Set Size
    request.setSize(size);
    SearchResponse responseWithHits;

    // Set sort field for search_after
    boolean ordered = select.isOrderdSelect();
    if (!ordered) {
      request.addSort(DOC_FIELD_NAME, ASC);
      request.addSort(METADATA_FIELD_ID, SortOrder.ASC);
    }
    // Set PIT
    request.setPointInTime(new PointInTimeBuilder(pit.getPitId()));
    // from and size is alternate method to paginate result.
    // If select has from clause, search after is not required.
    if (previousResponse != null && select.getFrom().isEmpty()) {
      request.searchAfter(previousResponse.getHits().getSortFields());
    }
    responseWithHits = request.get();

    return responseWithHits;
  }
}
