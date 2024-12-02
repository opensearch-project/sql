/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;

/**
 * AsyncQueryExecutorService exposes functionality to create, get results and cancel an async query.
 */
public interface AsyncQueryExecutorService {

  /**
   * Creates async query job based on the request and returns queryId in the response.
   *
   * @param createAsyncQueryRequest createAsyncQueryRequest.
   * @return {@link CreateAsyncQueryResponse}
   */
  CreateAsyncQueryResponse createAsyncQuery(
      CreateAsyncQueryRequest createAsyncQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Returns async query response for a given queryId.
   *
   * @param queryId queryId.
   * @return {@link AsyncQueryExecutionResponse}
   */
  AsyncQueryExecutionResponse getAsyncQueryResults(
      String queryId, AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Cancels running async query and returns the cancelled queryId.
   *
   * @param queryId queryId.
   * @return {@link String} cancelledQueryId.
   */
  String cancelQuery(String queryId, AsyncQueryRequestContext asyncQueryRequestContext);
}
