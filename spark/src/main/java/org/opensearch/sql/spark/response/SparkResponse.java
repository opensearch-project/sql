/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_INDEX_NAME;

import org.json.JSONObject;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

public class SparkResponse {
  private final Client client;
  private final String value;
  private final String field;

  /**
   * Response for spark sql query.
   *
   * @param client Opensearch client
   * @param value  Identifier field value
   * @param field  Identifier field name
   */
  public SparkResponse(Client client, String value, String field) {
    this.client = client;
    this.value = value;
    this.field = field;
  }

  public JSONObject getResultFromOpensearchIndex() {
    return searchInSparkIndex(QueryBuilders.termQuery(field, value));
  }

  private JSONObject searchInSparkIndex(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(SPARK_INDEX_NAME);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture;
    try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext()
        .stashContext()) {
      searchResponseActionFuture = client.search(searchRequest);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException(
          "Fetching result from " + SPARK_INDEX_NAME + " index failed with status : "
          + searchResponse.status());
    } else {
      JSONObject data = new JSONObject();
      for (SearchHit searchHit : searchResponse.getHits().getHits()) {
        data.put("data", searchHit.getSourceAsMap());
      }
      return data;
    }
  }
}
