/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.JOB_TYPE_TAG_KEY;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.utils.MetricUtils;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/**
 * The handler for batch query. With batch query, queries are executed as single batch. The queries
 * are sent along with job execution request ({@link StartJobRequest}) to spark.
 */
@RequiredArgsConstructor
public class BatchQueryHandler extends AsyncQueryHandler {
  protected final EMRServerlessClient emrServerlessClient;
  protected final JobExecutionResponseReader jobExecutionResponseReader;
  protected final LeaseManager leaseManager;

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
        asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId(), false);
    return asyncQueryJobMetadata.getQueryId().getId();
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    leaseManager.borrow(new LeaseRequest(JobType.BATCH, dispatchQueryRequest.getDatasource()));

    String clusterName = dispatchQueryRequest.getClusterName();
    Map<String, String> tags = context.getTags();
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();

    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    StartJobRequest startJobRequest =
        new StartJobRequest(
            clusterName + ":" + JobType.BATCH.getText(),
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.builder()
                .clusterName(clusterName)
                .dataSource(context.getDataSourceMetadata())
                .query(dispatchQueryRequest.getQuery())
                .build()
                .acceptModifier(dispatchQueryRequest.getSparkSubmitParameterModifier())
                .toString(),
            tags,
            false,
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    MetricUtils.incrementNumericalMetric(MetricName.EMR_BATCH_QUERY_JOBS_CREATION_COUNT);
    return new DispatchQueryResponse(
        context.getQueryId(), jobId, dataSourceMetadata.getResultIndex(), null);
  }
}
