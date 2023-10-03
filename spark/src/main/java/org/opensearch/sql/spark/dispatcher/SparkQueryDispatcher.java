/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;
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

  public String dispatch(DispatchQueryRequest dispatchQueryRequest) {
    return emrServerlessClient.startJobRun(getStartJobRequest(dispatchQueryRequest));
  }

  // TODO : Fetch from Result Index and then make call to EMR Serverless.
  public JSONObject getQueryResponse(String applicationId, String queryId) {
    GetJobRunResult getJobRunResult = emrServerlessClient.getJobRunResult(applicationId, queryId);
    JSONObject result = new JSONObject();
    if (getJobRunResult.getJobRun().getState().equals(JobRunState.SUCCESS.toString())) {
      result = jobExecutionResponseReader.getResultFromOpensearchIndex(queryId);
    }
    result.put("status", getJobRunResult.getJobRun().getState());
    return result;
  }

  public String cancelJob(String applicationId, String jobId) {
    CancelJobRunResult cancelJobRunResult = emrServerlessClient.cancelJobRun(applicationId, jobId);
    return cancelJobRunResult.getJobRunId();
  }

  // we currently don't support index queries in PPL language.
  // so we are treating all of them as non-index queries which don't require any kind of query
  // parsing.
  private StartJobRequest getStartJobRequest(DispatchQueryRequest dispatchQueryRequest) {
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())) {
      if (SQLQueryUtils.isIndexQuery(dispatchQueryRequest.getQuery()))
        return getStartJobRequestForIndexRequest(dispatchQueryRequest);
      else {
        return getStartJobRequestForNonIndexQueries(dispatchQueryRequest);
      }
    } else {
      return getStartJobRequestForNonIndexQueries(dispatchQueryRequest);
    }
  }

  private String getDataSourceRoleARN(DataSourceMetadata dataSourceMetadata) {
    if (DataSourceType.S3GLUE.equals(dataSourceMetadata.getConnector())) {
      return dataSourceMetadata.getProperties().get("glue.auth.role_arn");
    }
    throw new UnsupportedOperationException(
        String.format(
            "UnSupported datasource type for async queries:: %s",
            dataSourceMetadata.getConnector()));
  }

  private StartJobRequest getStartJobRequestForNonIndexQueries(
      DispatchQueryRequest dispatchQueryRequest) {
    StartJobRequest startJobRequest;
    dataSourceUserAuthorizationHelper.authorizeDataSource(
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource()));
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "non-index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource()))
                .build().toString(),
            tags, false);
    return startJobRequest;
  }

  private StartJobRequest getStartJobRequestForIndexRequest(
      DispatchQueryRequest dispatchQueryRequest) {
    StartJobRequest startJobRequest;
    IndexDetails indexDetails = SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    dataSourceUserAuthorizationHelper.authorizeDataSource(
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource()));
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    tags.put(INDEX_TAG_KEY, indexDetails.getIndexName());
    tags.put(TABLE_TAG_KEY, fullyQualifiedTableName.getTableName());
    tags.put(SCHEMA_TAG_KEY, fullyQualifiedTableName.getSchemaName());
    startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource()))
                .structuredStreaming()
                .build().toString(),
            tags, true);
    return startJobRequest;
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }
}
