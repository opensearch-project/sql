/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.client.EmrServerlessClient;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@ExtendWith(MockitoExtension.class)
public class SparkQueryDispatcherTest {

  @Mock private EmrServerlessClient emrServerlessClient;
  @Mock private DataSourceService dataSourceService;
  @Mock private JobExecutionResponseReader jobExecutionResponseReader;

  @Test
  void testDispatch() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient, dataSourceService, jobExecutionResponseReader);
    when(emrServerlessClient.startJobRun(
            QUERY,
            "flint-opensearch-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            constructExpectedSparkSubmitParameterString()))
        .thenReturn(EMR_JOB_ID);
    when(dataSourceService.getRawDataSourceMetadata("my_glue"))
        .thenReturn(constructMyGlueDataSourceMetadata());
    String jobId = sparkQueryDispatcher.dispatch(EMRS_APPLICATION_ID, QUERY, EMRS_EXECUTION_ROLE);
    verify(emrServerlessClient, times(1))
        .startJobRun(
            QUERY,
            "flint-opensearch-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            constructExpectedSparkSubmitParameterString());
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchWithWrongURI() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient, dataSourceService, jobExecutionResponseReader);
    when(dataSourceService.getRawDataSourceMetadata("my_glue"))
        .thenReturn(constructMyGlueDataSourceMetadataWithBadURISyntax());
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> sparkQueryDispatcher.dispatch(EMRS_APPLICATION_ID, QUERY, EMRS_EXECUTION_ROLE));
    Assertions.assertEquals(
        "Bad URI in indexstore configuration of the : my_glue datasoure.",
        illegalArgumentException.getMessage());
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadata() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("my_glue");
    dataSourceMetadata.setConnector(DataSourceType.S3GLUE);
    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put(
        "glue.auth.role_arn", "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole");
    properties.put(
        "glue.indexstore.opensearch.uri",
        "https://search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com");
    properties.put("glue.indexstore.opensearch.auth", "sigv4");
    properties.put("glue.indexstore.opensearch.region", "eu-west-1");
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadataWithBadURISyntax() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("my_glue");
    dataSourceMetadata.setConnector(DataSourceType.S3GLUE);
    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put(
        "glue.auth.role_arn", "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9090? param");
    properties.put("glue.indexstore.opensearch.auth", "sigv4");
    properties.put("glue.indexstore.opensearch.region", "eu-west-1");
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

  @Test
  void testGetQueryResponse() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient, dataSourceService, jobExecutionResponseReader);
    when(emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(new GetJobRunResult().withJobRun(new JobRun().withState(JobRunState.PENDING)));
    JSONObject result = sparkQueryDispatcher.getQueryResponse(EMRS_APPLICATION_ID, EMR_JOB_ID);
    Assertions.assertEquals("PENDING", result.get("status"));
    verifyNoInteractions(jobExecutionResponseReader);
  }

  @Test
  void testGetQueryResponseWithSuccess() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient, dataSourceService, jobExecutionResponseReader);
    when(emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(new GetJobRunResult().withJobRun(new JobRun().withState(JobRunState.SUCCESS)));
    JSONObject queryResult = new JSONObject();
    queryResult.put("data", "result");
    when(jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID))
        .thenReturn(queryResult);
    JSONObject result = sparkQueryDispatcher.getQueryResponse(EMRS_APPLICATION_ID, EMR_JOB_ID);
    verify(emrServerlessClient, times(1)).getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID);
    verify(jobExecutionResponseReader, times(1)).getResultFromOpensearchIndex(EMR_JOB_ID);
    Assertions.assertEquals(new HashSet<>(Arrays.asList("data", "status")), result.keySet());
    Assertions.assertEquals("result", result.get("data"));
    Assertions.assertEquals("SUCCESS", result.get("status"));
  }

  String constructExpectedSparkSubmitParameterString() {
    return " --class org.opensearch.sql.FlintJob  --conf"
               + " spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider"
               + "  --conf"
               + " spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory"
               + "  --conf"
               + " spark.jars=s3://flint-data-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar,s3://flint-data-dp-eu-west-1-beta/code/flint/flint-catalog.jar"
               + "  --conf"
               + " spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.1.0-SNAPSHOT"
               + "  --conf"
               + " spark.jars.repositories=https://aws.oss.sonatype.org/content/repositories/snapshots"
               + "  --conf"
               + " spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/"
               + "  --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/"
               + "  --conf"
               + " spark.datasource.flint.host=search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com"
               + "  --conf spark.datasource.flint.port=-1  --conf"
               + " spark.datasource.flint.scheme=https  --conf spark.datasource.flint.auth=sigv4 "
               + " --conf spark.datasource.flint.region=eu-west-1  --conf"
               + " spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider"
               + "  --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions "
               + " --conf"
               + " spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
               + "  --conf"
               + " spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
               + "  --conf"
               + " spark.emr-serverless.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
               + "  --conf"
               + " spark.hive.metastore.glue.role.arn=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
               + "  --conf spark.sql.catalog.my_glue=org.opensearch.sql.FlintDelegateCatalog ";
  }
}
