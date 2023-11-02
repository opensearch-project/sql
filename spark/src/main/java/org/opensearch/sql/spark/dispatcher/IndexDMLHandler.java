/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.execution.statestore.StateStore.createIndexDMLResult;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatementModelByQueryId;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.execution.session.SessionType;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.flint.operation.FlintIndexOp;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpCancel;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpDelete;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Handle Index DML query. includes * DROP * ALT? */
public class IndexDMLHandler extends AsyncQueryHandler {
  private static final Logger LOG = LogManager.getLogger();

  public static final String DROP_INDEX_JOB_ID = "dropIndexJobId";

  private final EMRServerlessClient emrServerlessClient;

  private final DataSourceService dataSourceService;

  private final DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  private final JobExecutionResponseReader jobExecutionResponseReader;

  private final FlintIndexMetadataReader flintIndexMetadataReader;

  private final Client client;

  private final StateStore stateStore;

  public IndexDMLHandler(
      EMRServerlessClient emrServerlessClient,
      DataSourceService dataSourceService,
      DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper,
      JobExecutionResponseReader jobExecutionResponseReader,
      FlintIndexMetadataReader flintIndexMetadataReader,
      Client client,
      StateStore stateStore) {
    super(jobExecutionResponseReader, stateStore);
    this.emrServerlessClient = emrServerlessClient;
    this.dataSourceService = dataSourceService;
    this.dataSourceUserAuthorizationHelper = dataSourceUserAuthorizationHelper;
    this.jobExecutionResponseReader = jobExecutionResponseReader;
    this.flintIndexMetadataReader = flintIndexMetadataReader;
    this.client = client;
    this.stateStore = stateStore;
  }

  public static boolean isIndexDMLQuery(String jobId) {
    return DROP_INDEX_JOB_ID.equalsIgnoreCase(jobId);
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    IndexQueryDetails indexDetails = context.getIndexQueryDetails();
    FlintIndexMetadata indexMetadata = flintIndexMetadataReader.getFlintIndexMetadata(indexDetails);
    // if index is created without auto refresh. there is no job to cancel.
    String status = JobRunState.FAILED.toString();
    String error = "";
    long startTime = 0L;
    try {
      FlintIndexOp jobCancelOp =
          new FlintIndexOpCancel(
              stateStore, dispatchQueryRequest.getDatasource(), emrServerlessClient);
      jobCancelOp.apply(indexMetadata);

      FlintIndexOp indexDeleteOp =
          new FlintIndexOpDelete(stateStore, dispatchQueryRequest.getDatasource());
      indexDeleteOp.apply(indexMetadata);
      status = JobRunState.SUCCESS.toString();
    } catch (Exception e) {
      error = e.getMessage();
      LOG.error(e);
    }

    AsyncQueryId asyncQueryId = AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName());
    IndexDMLResult indexDMLResult =
        new IndexDMLResult(
            asyncQueryId.getId(),
            status,
            error,
            dispatchQueryRequest.getDatasource(),
            System.currentTimeMillis() - startTime,
            System.currentTimeMillis());
    String resultIndex = dataSourceMetadata.getResultIndex();
    createIndexDMLResult(stateStore, resultIndex).apply(indexDMLResult);
    createSessionAndStatement(
        dispatchQueryRequest,
        dispatchQueryRequest.getApplicationId(),
        DROP_INDEX_JOB_ID,
        SessionType.BATCH,
        dataSourceMetadata.getName(),
        asyncQueryId);
    StateStore.updateStatementState(stateStore, asyncQueryId.getDataSourceName())
        .apply(
            getStatementModelByQueryId(stateStore, asyncQueryId),
            StatementState.fromString(status.toLowerCase(Locale.ROOT)));
    return new DispatchQueryResponse(asyncQueryId, DROP_INDEX_JOB_ID, resultIndex, null);
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    throw new IllegalArgumentException("can't cancel index DML query");
  }
}
