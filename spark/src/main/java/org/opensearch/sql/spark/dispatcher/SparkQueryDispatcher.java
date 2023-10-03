/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DELEGATE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_PASSWORD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_USERNAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_GLUE_ARN_KEY;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.S3GlueSparkSubmitParameters;
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

  private String constructSparkParameters(String datasourceName) {
    DataSourceMetadata dataSourceMetadata =
        dataSourceService.getRawDataSourceMetadata(datasourceName);
    S3GlueSparkSubmitParameters s3GlueSparkSubmitParameters = new S3GlueSparkSubmitParameters();
    s3GlueSparkSubmitParameters.addParameter(
        DRIVER_ENV_ASSUME_ROLE_ARN_KEY, getDataSourceRoleARN(dataSourceMetadata));
    s3GlueSparkSubmitParameters.addParameter(
        EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, getDataSourceRoleARN(dataSourceMetadata));
    s3GlueSparkSubmitParameters.addParameter(
        HIVE_METASTORE_GLUE_ARN_KEY, getDataSourceRoleARN(dataSourceMetadata));
    String opensearchuri = dataSourceMetadata.getProperties().get("glue.indexstore.opensearch.uri");
    URI uri;
    try {
      uri = new URI(opensearchuri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Bad URI in indexstore configuration of the : %s datasoure.", datasourceName));
    }
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
    s3GlueSparkSubmitParameters.addParameter(
        FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());
    s3GlueSparkSubmitParameters.addParameter(
        "spark.sql.catalog." + datasourceName, FLINT_DELEGATE_CATALOG);
    String auth = dataSourceMetadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH);
    setFlintIndexStoreAuthProperties(dataSourceMetadata, s3GlueSparkSubmitParameters, auth);
    return s3GlueSparkSubmitParameters.toString();
  }

  private static void setFlintIndexStoreAuthProperties(
      DataSourceMetadata dataSourceMetadata,
      S3GlueSparkSubmitParameters s3GlueSparkSubmitParameters,
      String authType) {
    if (AuthenticationType.get(authType).equals(AuthenticationType.BASICAUTH)) {
      s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_KEY, authType);
      String username =
          dataSourceMetadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME);
      String password =
          dataSourceMetadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD);
      s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_USERNAME, username);
      s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_PASSWORD, password);
    } else if (AuthenticationType.get(authType).equals(AuthenticationType.AWSSIGV4AUTH)) {
      String region = dataSourceMetadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_REGION);
      s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_KEY, "sigv4");
      s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AWSREGION_KEY, region);
    } else {
      s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_KEY, authType);
    }
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
            constructSparkParameters(dispatchQueryRequest.getDatasource()),
            tags);
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
            constructSparkParameters(dispatchQueryRequest.getDatasource()),
            tags);
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
