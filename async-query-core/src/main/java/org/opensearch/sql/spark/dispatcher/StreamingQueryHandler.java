/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.INDEX_TAG_KEY;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.JOB_TYPE_TAG_KEY;
import static org.opensearch.sql.spark.metrics.EmrMetrics.EMR_STREAMING_QUERY_JOBS_CREATION_COUNT;

import java.util.Map;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.QueryState;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/**
 * The handler for streaming query. Streaming query is a job to continuously update flint index.
 * Once started, the job can be stopped by IndexDML query.
 */
public class StreamingQueryHandler extends BatchQueryHandler {

  public StreamingQueryHandler(
      EMRServerlessClient emrServerlessClient,
      JobExecutionResponseReader jobExecutionResponseReader,
      LeaseManager leaseManager,
      MetricsService metricsService,
      SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider) {
    super(
        emrServerlessClient,
        jobExecutionResponseReader,
        leaseManager,
        metricsService,
        sparkSubmitParametersBuilderProvider);
  }

  @Override
  public String cancelJob(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    throw new IllegalArgumentException(
        "can't cancel index DML query, using ALTER auto_refresh=off statement to stop job, using"
            + " VACUUM statement to stop job and delete data");
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {

    leaseManager.borrow(new LeaseRequest(JobType.STREAMING, dispatchQueryRequest.getDatasource()));

    String clusterName = dispatchQueryRequest.getClusterName();
    IndexQueryDetails indexQueryDetails = context.getIndexQueryDetails();
    Map<String, String> tags = context.getTags();
    tags.put(INDEX_TAG_KEY, indexQueryDetails.openSearchIndexName());
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    String jobName =
        clusterName
            + ":"
            + JobType.STREAMING.getText()
            + ":"
            + indexQueryDetails.openSearchIndexName();
    StartJobRequest startJobRequest =
        new StartJobRequest(
            jobName,
            dispatchQueryRequest.getAccountId(),
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            sparkSubmitParametersBuilderProvider
                .getSparkSubmitParametersBuilder()
                .clusterName(clusterName)
                .query(dispatchQueryRequest.getQuery())
                .structuredStreaming(true)
                .dataSource(
                    dataSourceMetadata, dispatchQueryRequest, context.getAsyncQueryRequestContext())
                .acceptModifier(dispatchQueryRequest.getSparkSubmitParameterModifier())
                .acceptComposers(dispatchQueryRequest, context.getAsyncQueryRequestContext())
                .toString(),
            tags,
            indexQueryDetails.getFlintIndexOptions().autoRefresh(),
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    metricsService.incrementNumericalMetric(EMR_STREAMING_QUERY_JOBS_CREATION_COUNT);
    return DispatchQueryResponse.builder()
        .queryId(context.getQueryId())
        .jobId(jobId)
        .resultIndex(dataSourceMetadata.getResultIndex())
        .datasourceName(dataSourceMetadata.getName())
        .jobType(JobType.STREAMING)
        .indexName(indexQueryDetails.openSearchIndexName())
        .status(QueryState.WAITING)
        .build();
  }
}
