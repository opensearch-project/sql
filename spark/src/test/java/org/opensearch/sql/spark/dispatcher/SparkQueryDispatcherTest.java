/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_PASSWORD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_USERNAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
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
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class SparkQueryDispatcherTest {

  @Mock private EMRServerlessClient emrServerlessClient;
  @Mock private DataSourceService dataSourceService;
  @Mock private JobExecutionResponseReader jobExecutionResponseReader;
  @Mock private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  @Test
  void testDispatchSelectQuery() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    String query = "select * from my_glue.default.http_logs";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    }),
                tags, false)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    }),
                tags, false));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchSelectQueryWithBasicAuthIndexStoreDatasource() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    String query = "select * from my_glue.default.http_logs";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "basicauth",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AUTH_USERNAME, "username");
                        put(FLINT_INDEX_STORE_AUTH_PASSWORD, "password");
                      }
                    }),
                tags, false)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadataWithBasicAuth();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "basicauth",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AUTH_USERNAME, "username");
                        put(FLINT_INDEX_STORE_AUTH_PASSWORD, "password");
                      }
                    }),
                tags, false));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchSelectQueryWithNoAuthIndexStoreDatasource() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    String query = "select * from my_glue.default.http_logs";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "noauth",
                    new HashMap<>() {
                      {
                      }
                    }),
                tags, false)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadataWithNoAuth();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "noauth",
                    new HashMap<>() {
                      {
                      }
                    }),
                tags, false));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchIndexQuery() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("table", "http_logs");
    tags.put("index", "elb_and_requestUri");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("schema", "default");
    String query =
        "CREATE INDEX elb_and_requestUri ON my_glue.default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                withStructuredStreaming(constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    })),
                tags, true)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                withStructuredStreaming(constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    })),
                tags, true));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchWithPPLQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    String query = "source = my_glue.default.http_logs";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    }),
                tags, false)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.PPL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    }),
                tags, false));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchQueryWithoutATableAndDataSourceName() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    String query = "show tables";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    }),
                tags, false)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    }),
                tags, false));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchIndexQueryWithoutADatasourceName() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("table", "http_logs");
    tags.put("index", "elb_and_requestUri");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("schema", "default");
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    String query =
        "CREATE INDEX elb_and_requestUri ON default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                withStructuredStreaming(constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    })),
                tags, true)))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    String jobId =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1))
        .startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                withStructuredStreaming(constructExpectedSparkSubmitParameterString(
                    "sigv4",
                    new HashMap<>() {
                      {
                        put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                      }
                    })),
                tags, true));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testDispatchWithWrongURI() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    when(dataSourceService.getRawDataSourceMetadata("my_glue"))
        .thenReturn(constructMyGlueDataSourceMetadataWithBadURISyntax());
    String query = "select * from my_glue.default.http_logs";
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.dispatch(
                    new DispatchQueryRequest(
                        EMRS_APPLICATION_ID,
                        query,
                        "my_glue",
                        LangType.SQL,
                        EMRS_EXECUTION_ROLE,
                        TEST_CLUSTER_NAME)));
    Assertions.assertEquals(
        "Bad URI in indexstore configuration of the : my_glue datasoure.",
        illegalArgumentException.getMessage());
  }

  @Test
  void testDispatchWithUnSupportedDataSourceType() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    when(dataSourceService.getRawDataSourceMetadata("my_prometheus"))
        .thenReturn(constructPrometheusDataSourceType());
    String query = "select * from my_prometheus.default.http_logs";
    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                sparkQueryDispatcher.dispatch(
                    new DispatchQueryRequest(
                        EMRS_APPLICATION_ID,
                        query,
                        "my_prometheus",
                        LangType.SQL,
                        EMRS_EXECUTION_ROLE,
                        TEST_CLUSTER_NAME)));
    Assertions.assertEquals(
        "UnSupported datasource type for async queries:: PROMETHEUS",
        unsupportedOperationException.getMessage());
  }

  @Test
  void testCancelJob() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    String jobId = sparkQueryDispatcher.cancelJob(EMRS_APPLICATION_ID, EMR_JOB_ID);
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testGetQueryResponse() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
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
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader);
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

  private String constructExpectedSparkSubmitParameterString(
      String auth, Map<String, String> authParams) {
    StringBuilder authParamConfigBuilder = new StringBuilder();
    for (String key : authParams.keySet()) {
      authParamConfigBuilder.append(" --conf ");
      authParamConfigBuilder.append(key);
      authParamConfigBuilder.append("=");
      authParamConfigBuilder.append(authParams.get(key));
      authParamConfigBuilder.append(" ");
    }
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
               + " spark.datasource.flint.scheme=https  --conf spark.datasource.flint.auth="
        + auth
        + "  --conf"
        + " spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider"
        + "  --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions  --conf"
        + " spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        + "  --conf"
        + " spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
        + "  --conf"
        + " spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
        + "  --conf"
        + " spark.hive.metastore.glue.role.arn=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
        + "  --conf spark.sql.catalog.my_glue=org.opensearch.sql.FlintDelegateCatalog "
        + authParamConfigBuilder;
  }

  private String withStructuredStreaming(String parameters) {
    return parameters + " --conf spark.flint.job.type=streaming ";
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
    properties.put("glue.indexstore.opensearch.auth", "awssigv4");
    properties.put("glue.indexstore.opensearch.region", "eu-west-1");
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadataWithBasicAuth() {
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
    properties.put("glue.indexstore.opensearch.auth", "basicauth");
    properties.put("glue.indexstore.opensearch.auth.username", "username");
    properties.put("glue.indexstore.opensearch.auth.password", "password");
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadataWithNoAuth() {
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
    properties.put("glue.indexstore.opensearch.auth", "noauth");
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
    properties.put("glue.indexstore.opensearch.auth", "awssigv4");
    properties.put("glue.indexstore.opensearch.region", "eu-west-1");
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

  private DataSourceMetadata constructPrometheusDataSourceType() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("my_prometheus");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    Map<String, String> properties = new HashMap<>();
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }
}
