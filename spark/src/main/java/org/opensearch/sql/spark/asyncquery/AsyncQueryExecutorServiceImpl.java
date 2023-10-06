/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.common.setting.Settings.Key.CLUSTER_NAME;
import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
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
  private Settings settings;
  private Boolean isSparkJobExecutionEnabled;

  public AsyncQueryExecutorServiceImpl() {
    this.isSparkJobExecutionEnabled = Boolean.FALSE;
  }

  public AsyncQueryExecutorServiceImpl(
      AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService,
      SparkQueryDispatcher sparkQueryDispatcher,
      Settings settings) {
    this.isSparkJobExecutionEnabled = Boolean.TRUE;
    this.asyncQueryJobMetadataStorageService = asyncQueryJobMetadataStorageService;
    this.sparkQueryDispatcher = sparkQueryDispatcher;
    this.settings = settings;
  }

  @Override
  public CreateAsyncQueryResponse createAsyncQuery(
      CreateAsyncQueryRequest createAsyncQueryRequest) {
    validateSparkExecutionEngineSettings();
    String sparkExecutionEngineConfigString =
        settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG);
    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        AccessController.doPrivileged(
            (PrivilegedAction<SparkExecutionEngineConfig>)
                () ->
                    SparkExecutionEngineConfig.toSparkExecutionEngineConfig(
                        sparkExecutionEngineConfigString));
    ClusterName clusterName = settings.getSettingValue(CLUSTER_NAME);
    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                sparkExecutionEngineConfig.getApplicationId(),
                createAsyncQueryRequest.getQuery(),
                createAsyncQueryRequest.getDatasource(),
                createAsyncQueryRequest.getLang(),
                sparkExecutionEngineConfig.getExecutionRoleARN(),
                clusterName.value(),
                sparkExecutionEngineConfig.getSparkSubmitParameters()));
    asyncQueryJobMetadataStorageService.storeJobMetadata(
        new AsyncQueryJobMetadata(
            sparkExecutionEngineConfig.getApplicationId(),
            dispatchQueryResponse.getJobId(),
            dispatchQueryResponse.isDropIndexQuery(),
            dispatchQueryResponse.getResultIndex()));
    return new CreateAsyncQueryResponse(dispatchQueryResponse.getJobId());
  }

  @Override
  public AsyncQueryExecutionResponse getAsyncQueryResults(String queryId) {
    validateSparkExecutionEngineSettings();
    Optional<AsyncQueryJobMetadata> jobMetadata =
        asyncQueryJobMetadataStorageService.getJobMetadata(queryId);
    if (jobMetadata.isPresent()) {
      JSONObject jsonObject = sparkQueryDispatcher.getQueryResponse(jobMetadata.get());
      if (JobRunState.SUCCESS.toString().equals(jsonObject.getString(STATUS_FIELD))) {
        DefaultSparkSqlFunctionResponseHandle sparkSqlFunctionResponseHandle =
            new DefaultSparkSqlFunctionResponseHandle(jsonObject);
        List<ExprValue> result = new ArrayList<>();
        while (sparkSqlFunctionResponseHandle.hasNext()) {
          result.add(sparkSqlFunctionResponseHandle.next());
        }
        return new AsyncQueryExecutionResponse(
            JobRunState.SUCCESS.toString(), sparkSqlFunctionResponseHandle.schema(), result, null);
      } else {
        return new AsyncQueryExecutionResponse(
            jsonObject.optString(STATUS_FIELD, JobRunState.FAILED.toString()),
            null,
            null,
            jsonObject.optString(ERROR_FIELD, ""));
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

  private void validateSparkExecutionEngineSettings() {
    if (!isSparkJobExecutionEnabled) {
      throw new IllegalArgumentException(
          String.format(
              "Async Query APIs are disabled as %s is not configured in cluster settings. Please"
                  + " configure the setting and restart the domain to enable Async Query APIs",
              SPARK_EXECUTION_ENGINE_CONFIG.getKeyValue()));
    }
  }
}
