/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@RequiredArgsConstructor
public class BatchQueryHandler extends AsyncQueryHandler {
  private final EMRServerlessClient emrServerlessClient;
  private final JobExecutionResponseReader jobExecutionResponseReader;

  @Override
  protected JSONObject getResponseFromResultIndex(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    // either empty json when the result is not available or data with status
    // Fetch from Result Index
    return jobExecutionResponseReader.getResultFromOpensearchIndex(
        asyncQueryJobMetadata.getJobId(), asyncQueryJobMetadata.getResultIndex());
  }

  @Override
  protected JSONObject getResponseFromExecutor(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    JSONObject result = new JSONObject();
    // make call to EMR Serverless when related result index documents are not available
    GetJobRunResult getJobRunResult =
        emrServerlessClient.getJobRunResult(
            asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId());
    String jobState = getJobRunResult.getJobRun().getState();
    result.put(STATUS_FIELD, jobState);
    result.put(ERROR_FIELD, "");
    return result;
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    emrServerlessClient.cancelJobRun(
        asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId());
    return asyncQueryJobMetadata.getQueryId().getId();
  }
}
