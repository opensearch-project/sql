/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_RESPONSE_BUFFER_INDEX_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STEP_ID_FIELD;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

public class JobExecutionResponseReader {
  private final Client client;
  private static final Logger LOG = LogManager.getLogger();

  /**
   * JobExecutionResponseReader for spark query.
   *
   * @param client Opensearch client
   */
  public JobExecutionResponseReader(Client client) {
    this.client = client;
  }

  public JSONObject getResultFromOpensearchIndex(String jobId) {
    return searchInSparkIndex(QueryBuilders.termQuery(STEP_ID_FIELD, jobId));
  }

  private JSONObject searchInSparkIndex(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(SPARK_RESPONSE_BUFFER_INDEX_NAME);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture;
    try {
      searchResponseActionFuture = client.search(searchRequest);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException(
          "Fetching result from "
              + SPARK_RESPONSE_BUFFER_INDEX_NAME
              + " index failed with status : "
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
