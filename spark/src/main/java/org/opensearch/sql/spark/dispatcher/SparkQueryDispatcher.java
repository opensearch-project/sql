/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DELEGATE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_GLUE_ARN_KEY;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.client.EmrServerlessClient;
import org.opensearch.sql.spark.jobs.model.S3GlueSparkSubmitParameters;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  private EmrServerlessClient emrServerlessClient;

  private DataSourceService dataSourceService;

  private JobExecutionResponseReader jobExecutionResponseReader;

  public String dispatch(String applicationId, String query, String executionRoleARN) {
    String datasourceName = getDataSourceName();
    try {
      return emrServerlessClient.startJobRun(
          query,
          "flint-opensearch-query",
          applicationId,
          executionRoleARN,
          constructSparkParameters(datasourceName));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Bad URI in indexstore configuration of the : %s datasoure.", datasourceName));
    }
  }

  public JSONObject getQueryResponse(String applicationId, String jobId) {
    GetJobRunResult getJobRunResult = emrServerlessClient.getJobRunResult(applicationId, jobId);
    JSONObject result = new JSONObject();
    if (getJobRunResult.getJobRun().getState().equals(JobRunState.SUCCESS.toString())) {
      result = jobExecutionResponseReader.getResultFromOpensearchIndex(jobId);
    }
    result.put("status", getJobRunResult.getJobRun().getState());
    return result;
  }

  // TODO: Analyze given query
  // Extract datasourceName
  // Apply Authorizaiton.
  private String getDataSourceName() {
    return "my_glue";
  }

  private String getDataSourceRoleARN(DataSourceMetadata dataSourceMetadata) {
    return dataSourceMetadata.getProperties().get("glue.auth.role_arn");
  }

  private String constructSparkParameters(String datasourceName) throws URISyntaxException {
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
    URI uri = new URI(opensearchuri);
    String auth = dataSourceMetadata.getProperties().get("glue.indexstore.opensearch.auth");
    String region = dataSourceMetadata.getProperties().get("glue.indexstore.opensearch.region");
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
    s3GlueSparkSubmitParameters.addParameter(
        FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_KEY, auth);
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AWSREGION_KEY, region);
    s3GlueSparkSubmitParameters.addParameter(
        "spark.sql.catalog." + datasourceName, FLINT_DELEGATE_CATALOG);
    return s3GlueSparkSubmitParameters.toString();
  }
}
