/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.flint.operation.FlintIndexOp;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpCancel;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Handle Refresh Query. */
public class RefreshQueryHandler extends BatchQueryHandler {
  private static final Logger LOG = LogManager.getLogger();

  private final FlintIndexMetadataReader flintIndexMetadataReader;
  private final StateStore stateStore;
  private final EMRServerlessClient emrServerlessClient;

  public RefreshQueryHandler(
      EMRServerlessClient emrServerlessClient,
      JobExecutionResponseReader jobExecutionResponseReader,
      FlintIndexMetadataReader flintIndexMetadataReader,
      StateStore stateStore,
      LeaseManager leaseManager) {
    super(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    this.flintIndexMetadataReader = flintIndexMetadataReader;
    this.stateStore = stateStore;
    this.emrServerlessClient = emrServerlessClient;
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    String datasourceName = asyncQueryJobMetadata.getDatasourceName();
    FlintIndexMetadata indexMetadata =
        flintIndexMetadataReader.getFlintIndexMetadata(asyncQueryJobMetadata.getIndexName());
    try {
      FlintIndexOp jobCancelOp =
          new FlintIndexOpCancel(stateStore, datasourceName, emrServerlessClient);
      jobCancelOp.apply(indexMetadata);
    } catch (Exception e) {
      LOG.error(e);
    }
    return asyncQueryJobMetadata.getQueryId().getId();
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    DispatchQueryResponse resp = super.submit(dispatchQueryRequest, context);
    String indexName =
        context.getIndexQueryDetails() == null
            ? null
            : context.getIndexQueryDetails().openSearchIndexName();
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    return new DispatchQueryResponse(
        resp.getQueryId(),
        resp.getJobId(),
        resp.getResultIndex(),
        resp.getSessionId(),
        dataSourceMetadata.getName(),
        JobType.BATCH,
        indexName);
  }
}
