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
      FlintIndexOpFactory flintIndexOpFactory) {
    super(emrServerlessClient, jobExecutionResponseReader, leaseManager);
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
    return asyncQueryJobMetadata.getQueryId().getId();
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    DispatchQueryResponse resp = super.submit(dispatchQueryRequest, context);
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    return new DispatchQueryResponse(
        resp.getQueryId(),
        resp.getJobId(),
        resp.getResultIndex(),
        resp.getSessionId(),
        dataSourceMetadata.getName(),
        JobType.BATCH,
        context.getIndexQueryDetails().openSearchIndexName());
  }
}
