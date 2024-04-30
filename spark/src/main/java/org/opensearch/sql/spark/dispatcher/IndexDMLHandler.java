/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOp;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpAlter;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpDrop;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpVacuum;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Handle Index DML query. includes * DROP * ALT? */
@RequiredArgsConstructor
public class IndexDMLHandler extends AsyncQueryHandler {
  private static final Logger LOG = LogManager.getLogger();

  // To be deprecated in 3.0. Still using for backward compatibility.
  public static final String DROP_INDEX_JOB_ID = "dropIndexJobId";
  public static final String DML_QUERY_JOB_ID = "DMLQueryJobId";

  private final EMRServerlessClient emrServerlessClient;

  private final JobExecutionResponseReader jobExecutionResponseReader;

  private final FlintIndexMetadataService flintIndexMetadataService;

  private final FlintIndexStateModelService flintIndexStateModelService;
  private final IndexDMLResultStorageService indexDMLResultStorageService;

  public static boolean isIndexDMLQuery(String jobId) {
    return DROP_INDEX_JOB_ID.equalsIgnoreCase(jobId) || DML_QUERY_JOB_ID.equalsIgnoreCase(jobId);
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    long startTime = System.currentTimeMillis();
    try {
      IndexQueryDetails indexDetails = context.getIndexQueryDetails();
      FlintIndexMetadata indexMetadata = getFlintIndexMetadata(indexDetails);
      executeIndexOp(dispatchQueryRequest, indexDetails, indexMetadata);
      AsyncQueryId asyncQueryId =
          storeIndexDMLResult(
              dispatchQueryRequest,
              dataSourceMetadata,
              JobRunState.SUCCESS.toString(),
              StringUtils.EMPTY,
              startTime);
      return new DispatchQueryResponse(
          asyncQueryId, DML_QUERY_JOB_ID, dataSourceMetadata.getResultIndex(), null);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      AsyncQueryId asyncQueryId =
          storeIndexDMLResult(
              dispatchQueryRequest,
              dataSourceMetadata,
              JobRunState.FAILED.toString(),
              e.getMessage(),
              startTime);
      return new DispatchQueryResponse(
          asyncQueryId, DML_QUERY_JOB_ID, dataSourceMetadata.getResultIndex(), null);
    }
  }

  private AsyncQueryId storeIndexDMLResult(
      DispatchQueryRequest dispatchQueryRequest,
      DataSourceMetadata dataSourceMetadata,
      String status,
      String error,
      long startTime) {
    AsyncQueryId asyncQueryId = AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName());
    IndexDMLResult indexDMLResult =
        new IndexDMLResult(
            asyncQueryId.getId(),
            status,
            error,
            dispatchQueryRequest.getDatasource(),
            System.currentTimeMillis() - startTime,
            System.currentTimeMillis());
    indexDMLResultStorageService.createIndexDMLResult(indexDMLResult, dataSourceMetadata.getName());
    return asyncQueryId;
  }

  private void executeIndexOp(
      DispatchQueryRequest dispatchQueryRequest,
      IndexQueryDetails indexQueryDetails,
      FlintIndexMetadata indexMetadata) {
    switch (indexQueryDetails.getIndexQueryActionType()) {
      case DROP:
        FlintIndexOp dropOp =
            new FlintIndexOpDrop(
                flintIndexStateModelService,
                dispatchQueryRequest.getDatasource(),
                emrServerlessClient);
        dropOp.apply(indexMetadata);
        break;
      case ALTER:
        FlintIndexOpAlter flintIndexOpAlter =
            new FlintIndexOpAlter(
                indexQueryDetails.getFlintIndexOptions(),
                flintIndexStateModelService,
                dispatchQueryRequest.getDatasource(),
                emrServerlessClient,
                flintIndexMetadataService);
        flintIndexOpAlter.apply(indexMetadata);
        break;
      case VACUUM:
        FlintIndexOp indexVacuumOp =
            new FlintIndexOpVacuum(
                flintIndexStateModelService,
                dispatchQueryRequest.getDatasource(),
                flintIndexMetadataService);
        indexVacuumOp.apply(indexMetadata);
        break;
      default:
        throw new IllegalStateException(
            String.format(
                "IndexQueryActionType: %s is not supported in IndexDMLHandler.",
                indexQueryDetails.getIndexQueryActionType()));
    }
  }

  private FlintIndexMetadata getFlintIndexMetadata(IndexQueryDetails indexDetails) {
    Map<String, FlintIndexMetadata> indexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(indexDetails.openSearchIndexName());
    if (!indexMetadataMap.containsKey(indexDetails.openSearchIndexName())) {
      throw new IllegalStateException(
          String.format(
              "Couldn't fetch flint index: %s details", indexDetails.openSearchIndexName()));
    }
    return indexMetadataMap.get(indexDetails.openSearchIndexName());
  }

  @Override
  protected JSONObject getResponseFromResultIndex(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    String queryId = asyncQueryJobMetadata.getQueryId().getId();
    return jobExecutionResponseReader.getResultWithQueryId(
        queryId, asyncQueryJobMetadata.getResultLocation());
  }

  @Override
  protected JSONObject getResponseFromExecutor(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    // Consider statement still running if result doc created in submit() is not available yet
    JSONObject result = new JSONObject();
    result.put(STATUS_FIELD, StatementState.RUNNING.getState());
    result.put(ERROR_FIELD, "");
    return result;
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    throw new IllegalArgumentException("can't cancel index DML query");
  }
}
