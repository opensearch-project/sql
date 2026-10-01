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
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.job.Principal;
import org.opensearch.sql.job.QueryFailure;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.QueryJobService;
import org.opensearch.sql.job.QueryJobState;
import org.opensearch.sql.job.QueryJobStatus;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.SecurityAdapter;
import org.opensearch.sql.protocol.response.format.ExplainResponseJsonFormatter;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.QueryState;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.functions.response.DefaultSparkSqlFunctionResponseHandle;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;

/**
 * AsyncQueryExecutorService implementation of {@link AsyncQueryExecutorService}.
 *
 * <p>Also serves as the id-shape router in front of the in-JVM {@link QueryJobService} for PPL
 * async submissions per issue #5765. When {@link #queryJobService} is non-null and a queryId parses
 * as {@link QueryJobId}, get is dispatched to the neutral job service; the Spark path is otherwise
 * unchanged. This keeps the existing {@code /_plugins/_async_query} transport actions untouched and
 * avoids adding new REST endpoints or new transport {@code ActionType}s.
 */
public class AsyncQueryExecutorServiceImpl implements AsyncQueryExecutorService {
  private static final Schema EMPTY_SCHEMA = new Schema(List.of());

  private AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;
  private SparkQueryDispatcher sparkQueryDispatcher;
  private SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;
  private QueryJobService queryJobService;
  private SecurityAdapter securityAdapter;

  /**
   * Spark-only constructor. Retained for callers that do not wire the in-JVM job service (tests,
   * legacy composition).
   */
  public AsyncQueryExecutorServiceImpl(
      AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService,
      SparkQueryDispatcher sparkQueryDispatcher,
      SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier) {
    this(
        asyncQueryJobMetadataStorageService,
        sparkQueryDispatcher,
        sparkExecutionEngineConfigSupplier,
        null,
        null);
  }

  /**
   * Full constructor including the in-JVM job service and security adapter. When both are provided,
   * get dispatches to {@link QueryJobService} for ids that parse as {@link QueryJobId}; other ids
   * fall through to the Spark path.
   */
  public AsyncQueryExecutorServiceImpl(
      AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService,
      SparkQueryDispatcher sparkQueryDispatcher,
      SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier,
      QueryJobService queryJobService,
      SecurityAdapter securityAdapter) {
    this.asyncQueryJobMetadataStorageService = asyncQueryJobMetadataStorageService;
    this.sparkQueryDispatcher = sparkQueryDispatcher;
    this.sparkExecutionEngineConfigSupplier = sparkExecutionEngineConfigSupplier;
    this.queryJobService = queryJobService;
    this.securityAdapter = securityAdapter;
  }

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
            .query(createAsyncQueryRequest.getQuery())
            .langType(createAsyncQueryRequest.getLang())
            .state(dispatchQueryResponse.getStatus())
            .error(dispatchQueryResponse.getError())
            .build(),
        asyncQueryRequestContext);
    return new CreateAsyncQueryResponse(
        dispatchQueryResponse.getQueryId(), dispatchQueryResponse.getSessionId());
  }

  @Override
  public AsyncQueryExecutionResponse getAsyncQueryResults(
      String queryId, AsyncQueryRequestContext asyncQueryRequestContext) {
    Optional<QueryJobId> jobId = asJobId(queryId);
    if (jobId.isPresent() && queryJobService != null) {
      // Neutral exceptions (QueryJobNotFoundException / QueryJobForbiddenException) propagate to
      // TransportGetAsyncQueryResultAction, which translates them to transport-serializable
      // OpenSearchException subclasses so cross-node forwarding preserves the 404/403 status.
      return toAsyncResponse(queryJobService.get(jobId.get(), currentPrincipal()));
    }
    Optional<AsyncQueryJobMetadata> jobMetadata =
        asyncQueryJobMetadataStorageService.getJobMetadata(queryId);
    if (jobMetadata.isPresent()) {
      String sessionId = jobMetadata.get().getSessionId();
      JSONObject jsonObject =
          sparkQueryDispatcher.getQueryResponse(jobMetadata.get(), asyncQueryRequestContext);
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
            sessionId,
            null);
      } else {
        return new AsyncQueryExecutionResponse(
            jsonObject.optString(STATUS_FIELD, JobRunState.FAILED.toString()),
            null,
            null,
            jsonObject.optString(ERROR_FIELD, ""),
            sessionId,
            null);
      }
    }
    throw new AsyncQueryNotFoundException(String.format("QueryId: %s not found", queryId));
  }

  @Override
  public String cancelQuery(String queryId, AsyncQueryRequestContext asyncQueryRequestContext) {
    Optional<AsyncQueryJobMetadata> asyncQueryJobMetadata =
        asyncQueryJobMetadataStorageService.getJobMetadata(queryId);
    if (asyncQueryJobMetadata.isPresent()) {
      String result =
          sparkQueryDispatcher.cancelJob(asyncQueryJobMetadata.get(), asyncQueryRequestContext);
      asyncQueryJobMetadataStorageService.updateState(
          asyncQueryJobMetadata.get(), QueryState.CANCELLED, asyncQueryRequestContext);
      return result;
    }
    throw new AsyncQueryNotFoundException(String.format("QueryId: %s not found", queryId));
  }

  /**
   * Parses {@code id} as a {@link QueryJobId} or returns empty if the id does not match the opaque
   * layout. Spark job ids never accidentally satisfy the versioned, length-prefixed layout.
   */
  private static Optional<QueryJobId> asJobId(String id) {
    if (id == null || id.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(QueryJobId.parse(id));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  private Principal currentPrincipal() {
    return securityAdapter != null ? securityAdapter.current() : Principal.UNSECURED;
  }

  /**
   * Maps a neutral {@link QueryJobStatus} onto the response shape the async-query transport actions
   * already know how to format. Terminal SUCCEEDED carries schema and rows for the {@link
   * QueryResult.Rows} variant, or the pre-formatted explain JSON in {@code explainJson} for the
   * {@link QueryResult.Explain} variant. FAILED carries a sanitized error; RUNNING / PENDING /
   * CANCELLED carry no rows.
   */
  private static AsyncQueryExecutionResponse toAsyncResponse(QueryJobStatus status) {
    if (status.state() == QueryJobState.SUCCEEDED && status.result().isPresent()) {
      QueryResult result = status.result().get();
      if (result instanceof QueryResult.Rows rows) {
        return new AsyncQueryExecutionResponse(
            status.state().name(), rows.schema(), rows.rows(), null, null, null);
      }
      if (result instanceof QueryResult.Explain explain) {
        return new AsyncQueryExecutionResponse(
            status.state().name(),
            EMPTY_SCHEMA,
            List.of(),
            null,
            null,
            new ExplainResponseJsonFormatter(JsonResponseFormatter.Style.PRETTY)
                .format(explain.response()));
      }
    }
    if (status.state() == QueryJobState.FAILED) {
      return new AsyncQueryExecutionResponse(
          status.state().name(),
          EMPTY_SCHEMA,
          List.of(),
          status.failure().map(QueryFailure::reason).orElse("query execution failed"),
          null,
          null);
    }
    return new AsyncQueryExecutionResponse(
        status.state().name(), EMPTY_SCHEMA, List.of(), null, null, null);
  }
}
