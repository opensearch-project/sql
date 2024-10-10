/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import org.json.JSONObject;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

/** Interface for reading job execution result */
public interface JobExecutionResponseReader {
  /**
   * Retrieves the job execution result based on the job ID.
   *
   * @param asyncQueryJobMetadata metadata will have jobId and resultLocation and other required
   *     params.
   * @return A JSONObject containing the result data.
   */
  JSONObject getResultFromResultIndex(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Retrieves the job execution result based on the query ID.
   *
   * @param queryId The query ID.
   * @param resultLocation The location identifier where the result is stored (optional).
   * @return A JSONObject containing the result data.
   */
  JSONObject getResultWithQueryId(
      String queryId, String resultLocation, AsyncQueryRequestContext asyncQueryRequestContext);
}
