/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import java.util.Map;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOp;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/**
 * The handler for refresh query. Refresh query is one time query request to refresh(update) flint
 * index, and new job is submitted to Spark.
 */
public class RefreshQueryHandler extends BatchQueryHandler {

  private final FlintIndexMetadataService flintIndexMetadataService;
  private final FlintIndexOpFactory flintIndexOpFactory;

  public RefreshQueryHandler(
      EMRServerlessClient emrServerlessClient,
      JobExecutionResponseReader jobExecutionResponseReader,
      FlintIndexMetadataService flintIndexMetadataService,
      LeaseManager leaseManager,
      FlintIndexOpFactory flintIndexOpFactory,
      MetricsService metricsService,
      SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider) {
    super(
        emrServerlessClient,
        jobExecutionResponseReader,
        leaseManager,
        metricsService,
        sparkSubmitParametersBuilderProvider);
    this.flintIndexMetadataService = flintIndexMetadataService;
    this.flintIndexOpFactory = flintIndexOpFactory;
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    String datasourceName = asyncQueryJobMetadata.getDatasourceName();
    Map<String, FlintIndexMetadata> indexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(asyncQueryJobMetadata.getIndexName());
    if (!indexMetadataMap.containsKey(asyncQueryJobMetadata.getIndexName())) {
      throw new IllegalStateException(
          String.format(
              "Couldn't fetch flint index: %s details", asyncQueryJobMetadata.getIndexName()));
    }
    FlintIndexMetadata indexMetadata = indexMetadataMap.get(asyncQueryJobMetadata.getIndexName());
    FlintIndexOp jobCancelOp = flintIndexOpFactory.getCancel(datasourceName);
    jobCancelOp.apply(indexMetadata);
    return asyncQueryJobMetadata.getQueryId();
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    leaseManager.borrow(new LeaseRequest(JobType.BATCH, dispatchQueryRequest.getDatasource()));

    DispatchQueryResponse resp = super.submit(dispatchQueryRequest, context);
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    return DispatchQueryResponse.builder()
        .queryId(resp.getQueryId())
        .jobId(resp.getJobId())
        .resultIndex(resp.getResultIndex())
        .sessionId(resp.getSessionId())
        .datasourceName(dataSourceMetadata.getName())
        .jobType(JobType.BATCH)
        .indexName(context.getIndexQueryDetails().openSearchIndexName())
        .build();
  }
}
