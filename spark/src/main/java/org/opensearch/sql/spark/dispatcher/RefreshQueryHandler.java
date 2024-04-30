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
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOp;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpCancel;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Handle Refresh Query. */
public class RefreshQueryHandler extends BatchQueryHandler {

  private final FlintIndexMetadataService flintIndexMetadataService;
  private final FlintIndexStateModelService flintIndexStateModelService;
  private final EMRServerlessClient emrServerlessClient;

  public RefreshQueryHandler(
      EMRServerlessClient emrServerlessClient,
      JobExecutionResponseReader jobExecutionResponseReader,
      FlintIndexMetadataService flintIndexMetadataService,
      FlintIndexStateModelService flintIndexStateModelService,
      LeaseManager leaseManager) {
    super(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    this.flintIndexMetadataService = flintIndexMetadataService;
    this.flintIndexStateModelService = flintIndexStateModelService;
    this.emrServerlessClient = emrServerlessClient;
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
    FlintIndexOp jobCancelOp =
        new FlintIndexOpCancel(flintIndexStateModelService, datasourceName, emrServerlessClient);
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
