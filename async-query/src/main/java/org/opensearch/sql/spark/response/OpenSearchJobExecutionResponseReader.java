/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.JOB_ID_FIELD;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

/** JobExecutionResponseReader implementation for reading response from OpenSearch index. */
public class OpenSearchJobExecutionResponseReader implements JobExecutionResponseReader {
  private final Client client;
  private static final Logger LOG = LogManager.getLogger();

  public OpenSearchJobExecutionResponseReader(Client client) {
    this.client = client;
  }

  @Override
  public JSONObject getResultFromResultIndex(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    return searchInSparkIndex(
        QueryBuilders.termQuery(JOB_ID_FIELD, asyncQueryJobMetadata.getJobId()),
        asyncQueryJobMetadata.getResultIndex());
  }

  @Override
  public JSONObject getResultWithQueryId(
      String queryId, String resultLocation, AsyncQueryRequestContext asyncQueryRequestContext) {
    return searchInSparkIndex(QueryBuilders.termQuery("queryId", queryId), resultLocation);
  }

  private JSONObject searchInSparkIndex(QueryBuilder query, String resultIndex) {
    SearchRequest searchRequest = new SearchRequest();
    String searchResultIndex = resultIndex == null ? DEFAULT_RESULT_INDEX : resultIndex;
    searchRequest.indices(searchResultIndex);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture;
    JSONObject data = new JSONObject();
    try {
      searchResponseActionFuture = client.search(searchRequest);
    } catch (IndexNotFoundException e) {
      // if there is no result index (e.g., EMR-S hasn't created the index yet), we return empty
      // json
      LOG.info(resultIndex + " is not created yet.");
      return data;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException(
          "Fetching result from "
              + searchResultIndex
              + " index failed with status : "
              + searchResponse.status());
    } else {
      for (SearchHit searchHit : searchResponse.getHits().getHits()) {
        data.put(DATA_FIELD, searchHit.getSourceAsMap());
      }
      return data;
    }
  }
}
