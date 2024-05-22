/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import org.json.JSONObject;

/** Interface for reading job execution result */
public interface JobExecutionResponseReader {
  /**
   * Retrieves the result from the OpenSearch index based on the job ID.
   *
   * @param jobId The job ID.
   * @param resultLocation The location identifier where the result is stored (optional).
   * @return A JSONObject containing the result data.
   */
  JSONObject getResultWithJobId(String jobId, String resultLocation);

  /**
   * Retrieves the result from the OpenSearch index based on the query ID.
   *
   * @param queryId The query ID.
   * @param resultLocation The location identifier where the result is stored (optional).
   * @return A JSONObject containing the result data.
   */
  JSONObject getResultWithQueryId(String queryId, String resultLocation);
}
