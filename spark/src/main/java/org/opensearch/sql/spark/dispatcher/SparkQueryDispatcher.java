/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  public static final String INDEX_TAG_KEY = "index";
  public static final String DATASOURCE_TAG_KEY = "datasource";
  public static final String SCHEMA_TAG_KEY = "schema";
  public static final String TABLE_TAG_KEY = "table";
  public static final String CLUSTER_NAME_TAG_KEY = "cluster";

  private EMRServerlessClient emrServerlessClient;

  private DataSourceService dataSourceService;

  private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  private JobExecutionResponseReader jobExecutionResponseReader;

  private FlintIndexMetadataReader flintIndexMetadataReader;

  public DispatchQueryResponse dispatch(DispatchQueryRequest dispatchQueryRequest) {
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())) {
      return handleSQLQuery(dispatchQueryRequest);
    } else {
      // Since we don't need any extra handling for PPL, we are treating it as normal dispatch
      // Query.
      return handleNonIndexQuery(dispatchQueryRequest);
    }
  }

  // TODO : Fetch from Result Index and then make call to EMR Serverless.
  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    GetJobRunResult getJobRunResult =
        emrServerlessClient.getJobRunResult(
            asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId());
    String jobState = getJobRunResult.getJobRun().getState();
    JSONObject result =
        (jobState.equals(JobRunState.SUCCESS.toString()))
            ? jobExecutionResponseReader.getResultFromOpensearchIndex(
                asyncQueryJobMetadata.getJobId(), asyncQueryJobMetadata.getResultIndex())
            : new JSONObject();

    // if result index document has a status, we are gonna use the status directly; otherwise, we
    // will use emr-s job status
    // a job is successful does not mean there is no error in execution. For example, even if result
    // index mapping
    // is incorrect, we still write query result and let the job finish.
    if (result.has(DATA_FIELD)) {
      JSONObject items = result.getJSONObject(DATA_FIELD);

      // If items have STATUS_FIELD, use it; otherwise, use jobState
      String status = items.optString(STATUS_FIELD, jobState);
      result.put(STATUS_FIELD, status);

      // If items have ERROR_FIELD, use it; otherwise, set empty string
      String error = items.optString(ERROR_FIELD, "");
      result.put(ERROR_FIELD, error);
    } else {
      result.put(STATUS_FIELD, jobState);
      result.put(ERROR_FIELD, "");
    }

    return result;
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    CancelJobRunResult cancelJobRunResult =
        emrServerlessClient.cancelJobRun(
            asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId());
    return cancelJobRunResult.getJobRunId();
  }

  private DispatchQueryResponse handleSQLQuery(DispatchQueryRequest dispatchQueryRequest) {
    if (SQLQueryUtils.isIndexQuery(dispatchQueryRequest.getQuery())) {
      IndexDetails indexDetails =
          SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
      if (indexDetails.isDropIndex()) {
        return handleDropIndexQuery(dispatchQueryRequest, indexDetails);
      } else {
        return handleIndexQuery(dispatchQueryRequest, indexDetails);
      }
    } else {
      return handleNonIndexQuery(dispatchQueryRequest);
    }
  }

  private DispatchQueryResponse handleIndexQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexDetails indexDetails) {
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    tags.put(INDEX_TAG_KEY, indexDetails.getIndexName());
    tags.put(TABLE_TAG_KEY, fullyQualifiedTableName.getTableName());
    tags.put(SCHEMA_TAG_KEY, fullyQualifiedTableName.getSchemaName());
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(
                    dataSourceService.getRawDataSourceMetadata(
                        dispatchQueryRequest.getDatasource()))
                .structuredStreaming(indexDetails.getAutoRefresh())
                .build()
                .toString(),
            tags,
            indexDetails.getAutoRefresh(),
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    return new DispatchQueryResponse(jobId, false, dataSourceMetadata.getResultIndex());
  }

  private DispatchQueryResponse handleNonIndexQuery(DispatchQueryRequest dispatchQueryRequest) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "non-index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(
                    dataSourceService.getRawDataSourceMetadata(
                        dispatchQueryRequest.getDatasource()))
                .build()
                .toString(),
            tags,
            false,
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    return new DispatchQueryResponse(jobId, false, dataSourceMetadata.getResultIndex());
  }

  private DispatchQueryResponse handleDropIndexQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexDetails indexDetails) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobId = flintIndexMetadataReader.getJobIdFromFlintIndexMetadata(indexDetails);
    emrServerlessClient.cancelJobRun(dispatchQueryRequest.getApplicationId(), jobId);
    String dropIndexDummyJobId = RandomStringUtils.randomAlphanumeric(16);
    return new DispatchQueryResponse(
        dropIndexDummyJobId, true, dataSourceMetadata.getResultIndex());
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }
}
