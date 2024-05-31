/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.functions.response.DefaultSparkSqlFunctionResponseHandle;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;

/** AsyncQueryExecutorService implementation of {@link AsyncQueryExecutorService}. */
@AllArgsConstructor
public class AsyncQueryExecutorServiceImpl implements AsyncQueryExecutorService {
  private AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;
  private SparkQueryDispatcher sparkQueryDispatcher;
  private SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;

  @Override
  public CreateAsyncQueryResponse createAsyncQuery(
      CreateAsyncQueryRequest createAsyncQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(asyncQueryRequestContext);
    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            DispatchQueryRequest.builder()
                .accountId(sparkExecutionEngineConfig.getAccountId())
                .applicationId(sparkExecutionEngineConfig.getApplicationId())
                .query(createAsyncQueryRequest.getQuery())
                .datasource(createAsyncQueryRequest.getDatasource())
                .langType(createAsyncQueryRequest.getLang())
                .executionRoleARN(sparkExecutionEngineConfig.getExecutionRoleARN())
                .clusterName(sparkExecutionEngineConfig.getClusterName())
                .sparkSubmitParameterModifier(
                    sparkExecutionEngineConfig.getSparkSubmitParameterModifier())
                .sessionId(createAsyncQueryRequest.getSessionId())
                .build(),
            asyncQueryRequestContext);
    asyncQueryJobMetadataStorageService.storeJobMetadata(
        AsyncQueryJobMetadata.builder()
            .queryId(dispatchQueryResponse.getQueryId())
            .accountId(sparkExecutionEngineConfig.getAccountId())
            .applicationId(sparkExecutionEngineConfig.getApplicationId())
            .jobId(dispatchQueryResponse.getJobId())
            .resultIndex(dispatchQueryResponse.getResultIndex())
            .sessionId(dispatchQueryResponse.getSessionId())
            .datasourceName(dispatchQueryResponse.getDatasourceName())
            .jobType(dispatchQueryResponse.getJobType())
            .indexName(dispatchQueryResponse.getIndexName())
            .build(),
        asyncQueryRequestContext);
    return new CreateAsyncQueryResponse(
        dispatchQueryResponse.getQueryId(), dispatchQueryResponse.getSessionId());
  }

  @Override
  public AsyncQueryExecutionResponse getAsyncQueryResults(String queryId) {
    Optional<AsyncQueryJobMetadata> jobMetadata =
        asyncQueryJobMetadataStorageService.getJobMetadata(queryId);
    if (jobMetadata.isPresent()) {
      String sessionId = jobMetadata.get().getSessionId();
      JSONObject jsonObject = sparkQueryDispatcher.getQueryResponse(jobMetadata.get());
      if (JobRunState.SUCCESS.toString().equals(jsonObject.getString(STATUS_FIELD))) {
        DefaultSparkSqlFunctionResponseHandle sparkSqlFunctionResponseHandle =
            new DefaultSparkSqlFunctionResponseHandle(jsonObject);
        List<ExprValue> result = new ArrayList<>();
        while (sparkSqlFunctionResponseHandle.hasNext()) {
          result.add(sparkSqlFunctionResponseHandle.next());
        }
        return new AsyncQueryExecutionResponse(
            JobRunState.SUCCESS.toString(),
            sparkSqlFunctionResponseHandle.schema(),
            result,
            null,
            sessionId);
      } else {
        return new AsyncQueryExecutionResponse(
            jsonObject.optString(STATUS_FIELD, JobRunState.FAILED.toString()),
            null,
            null,
            jsonObject.optString(ERROR_FIELD, ""),
            sessionId);
      }
    }
    throw new AsyncQueryNotFoundException(String.format("QueryId: %s not found", queryId));
  }

  @Override
  public String cancelQuery(String queryId) {
    Optional<AsyncQueryJobMetadata> asyncQueryJobMetadata =
        asyncQueryJobMetadataStorageService.getJobMetadata(queryId);
    if (asyncQueryJobMetadata.isPresent()) {
      return sparkQueryDispatcher.cancelJob(asyncQueryJobMetadata.get());
    }
    throw new AsyncQueryNotFoundException(String.format("QueryId: %s not found", queryId));
  }
}
