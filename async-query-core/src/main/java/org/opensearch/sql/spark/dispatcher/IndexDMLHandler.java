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
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.QueryState;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOp;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/**
 * The handler for Index DML (Data Manipulation Language) query. Handles DROP/ALTER/VACUUM operation
 * for flint indices. It will stop streaming query job as needed (e.g. when the flint index is
 * automatically updated by a streaming query, the streaming query is stopped when the index is
 * dropped)
 */
@RequiredArgsConstructor
public class IndexDMLHandler extends AsyncQueryHandler {
  private static final Logger LOG = LogManager.getLogger();

  // To be deprecated in 3.0. Still using for backward compatibility.
  public static final String DROP_INDEX_JOB_ID = "dropIndexJobId";
  public static final String DML_QUERY_JOB_ID = "DMLQueryJobId";

  private final JobExecutionResponseReader jobExecutionResponseReader;
  private final FlintIndexMetadataService flintIndexMetadataService;
  private final IndexDMLResultStorageService indexDMLResultStorageService;
  private final FlintIndexOpFactory flintIndexOpFactory;

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
      FlintIndexMetadata indexMetadata =
          getFlintIndexMetadata(indexDetails, context.getAsyncQueryRequestContext());

      getIndexOp(dispatchQueryRequest, indexDetails)
          .apply(indexMetadata, context.getAsyncQueryRequestContext());

      String asyncQueryId =
          storeIndexDMLResult(
              context.getQueryId(),
              dispatchQueryRequest,
              dataSourceMetadata,
              JobRunState.SUCCESS.toString(),
              StringUtils.EMPTY,
              getElapsedTimeSince(startTime),
              context.getAsyncQueryRequestContext());
      return DispatchQueryResponse.builder()
          .queryId(asyncQueryId)
          .jobId(DML_QUERY_JOB_ID)
          .resultIndex(dataSourceMetadata.getResultIndex())
          .datasourceName(dataSourceMetadata.getName())
          .jobType(JobType.BATCH)
          .status(QueryState.SUCCESS)
          .build();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      String asyncQueryId =
          storeIndexDMLResult(
              context.getQueryId(),
              dispatchQueryRequest,
              dataSourceMetadata,
              JobRunState.FAILED.toString(),
              e.getMessage(),
              getElapsedTimeSince(startTime),
              context.getAsyncQueryRequestContext());
      return DispatchQueryResponse.builder()
          .queryId(asyncQueryId)
          .jobId(DML_QUERY_JOB_ID)
          .resultIndex(dataSourceMetadata.getResultIndex())
          .datasourceName(dataSourceMetadata.getName())
          .jobType(JobType.BATCH)
          .status(QueryState.FAILED)
          .error(e.getMessage())
          .build();
    }
  }

  private String storeIndexDMLResult(
      String queryId,
      DispatchQueryRequest dispatchQueryRequest,
      DataSourceMetadata dataSourceMetadata,
      String status,
      String error,
      long queryRunTime,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    IndexDMLResult indexDMLResult =
        IndexDMLResult.builder()
            .queryId(queryId)
            .status(status)
            .error(error)
            .datasourceName(dispatchQueryRequest.getDatasource())
            .queryRunTime(queryRunTime)
            .updateTime(System.currentTimeMillis())
            .build();
    indexDMLResultStorageService.createIndexDMLResult(indexDMLResult, asyncQueryRequestContext);
    return queryId;
  }

  private long getElapsedTimeSince(long startTime) {
    return System.currentTimeMillis() - startTime;
  }

  private FlintIndexOp getIndexOp(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    switch (indexQueryDetails.getIndexQueryActionType()) {
      case DROP:
        return flintIndexOpFactory.getDrop(dispatchQueryRequest.getDatasource());
      case ALTER:
        return flintIndexOpFactory.getAlter(
            indexQueryDetails.getFlintIndexOptions(), dispatchQueryRequest.getDatasource());
      case VACUUM:
        return flintIndexOpFactory.getVacuum(dispatchQueryRequest.getDatasource());
      default:
        throw new IllegalStateException(
            String.format(
                "IndexQueryActionType: %s is not supported in IndexDMLHandler.",
                indexQueryDetails.getIndexQueryActionType()));
    }
  }

  private FlintIndexMetadata getFlintIndexMetadata(
      IndexQueryDetails indexDetails, AsyncQueryRequestContext asyncQueryRequestContext) {
    Map<String, FlintIndexMetadata> indexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(
            indexDetails.openSearchIndexName(), asyncQueryRequestContext);
    if (!indexMetadataMap.containsKey(indexDetails.openSearchIndexName())) {
      throw new IllegalStateException(
          String.format(
              "Couldn't fetch flint index: %s details", indexDetails.openSearchIndexName()));
    }
    return indexMetadataMap.get(indexDetails.openSearchIndexName());
  }

  @Override
  protected JSONObject getResponseFromResultIndex(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    String queryId = asyncQueryJobMetadata.getQueryId();
    return jobExecutionResponseReader.getResultWithQueryId(
        queryId, asyncQueryJobMetadata.getResultIndex());
  }

  @Override
  protected JSONObject getResponseFromExecutor(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    // Consider statement still running if result doc created in submit() is not available yet
    JSONObject result = new JSONObject();
    result.put(STATUS_FIELD, StatementState.RUNNING.getState());
    result.put(ERROR_FIELD, "");
    return result;
  }

  @Override
  public String cancelJob(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    throw new IllegalArgumentException("can't cancel index DML query");
  }
}
