/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.JOB_TYPE_TAG_KEY;

import java.util.Map;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.utils.MetricUtils;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.SessionType;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

public class BatchQueryHandler extends AsyncQueryHandler {
  private final EMRServerlessClient emrServerlessClient;
  private final JobExecutionResponseReader jobExecutionResponseReader;
  private final StateStore stateStore;
  protected final LeaseManager leaseManager;

  public BatchQueryHandler(
      EMRServerlessClient emrServerlessClient,
      StateStore stateStore,
      JobExecutionResponseReader jobExecutionResponseReader,
      LeaseManager leaseManager) {
    super(jobExecutionResponseReader, stateStore);
    this.emrServerlessClient = emrServerlessClient;
    this.jobExecutionResponseReader = jobExecutionResponseReader;
    this.stateStore = stateStore;
    this.leaseManager = leaseManager;
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    leaseManager.borrow(new LeaseRequest(JobType.BATCH, dispatchQueryRequest.getDatasource()));

    String jobName = dispatchQueryRequest.getClusterName() + ":" + "non-index-query";
    Map<String, String> tags = context.getTags();
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();

    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(context.getDataSourceMetadata())
                .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                .build()
                .toString(),
            tags,
            false,
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    createSessionAndStatement(
        dispatchQueryRequest,
        dispatchQueryRequest.getApplicationId(),
        jobId,
        SessionType.BATCH,
        dataSourceMetadata.getName(),
        context.getQueryId());
    MetricUtils.incrementNumericalMetric(MetricName.EMR_BATCH_QUERY_JOBS_CREATION_COUNT);
    return new DispatchQueryResponse(
        context.getQueryId(), jobId, dataSourceMetadata.getResultIndex(), null);
  }
}
