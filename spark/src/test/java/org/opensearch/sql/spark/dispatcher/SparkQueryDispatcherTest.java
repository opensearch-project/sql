/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_PASSWORD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_USERNAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class SparkQueryDispatcherTest {

  @Mock private EMRServerlessClient emrServerlessClient;
  @Mock private DataSourceService dataSourceService;
  @Mock private JobExecutionResponseReader jobExecutionResponseReader;
  @Mock private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;
  @Mock private FlintIndexMetadataReader flintIndexMetadataReader;

  private SparkQueryDispatcher sparkQueryDispatcher;

  @BeforeEach
  void setUp() {
    sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader,
            flintIndexMetadataReader);
  }

  @Test
  void testDispatchSelectQuery() {
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
                tags,
                false,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                tags,
                false,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchSelectQueryWithBasicAuthIndexStoreDatasource() {
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
                tags,
                false,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadataWithBasicAuth();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                tags,
                false,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchSelectQueryWithNoAuthIndexStoreDatasource() {
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
                tags,
                false,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadataWithNoAuth();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                tags,
                false,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchIndexQuery() {
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
                withStructuredStreaming(
                    constructExpectedSparkSubmitParameterString(
                        "sigv4",
                        new HashMap<>() {
                          {
                            put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                          }
                        })),
                tags,
                true,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                withStructuredStreaming(
                    constructExpectedSparkSubmitParameterString(
                        "sigv4",
                        new HashMap<>() {
                          {
                            put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                          }
                        })),
                tags,
                true,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchWithPPLQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);

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
                tags,
                false,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                tags,
                false,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchQueryWithoutATableAndDataSourceName() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);

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
                tags,
                false,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                tags,
                false,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchIndexQueryWithoutADatasourceName() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("table", "http_logs");
    tags.put("index", "elb_and_requestUri");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("schema", "default");

    String query =
        "CREATE INDEX elb_and_requestUri ON default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                withStructuredStreaming(
                    constructExpectedSparkSubmitParameterString(
                        "sigv4",
                        new HashMap<>() {
                          {
                            put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                          }
                        })),
                tags,
                true,
                any())))
        .thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
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
                withStructuredStreaming(
                    constructExpectedSparkSubmitParameterString(
                        "sigv4",
                        new HashMap<>() {
                          {
                            put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                          }
                        })),
                tags,
                true,
                any()));
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchWithWrongURI() {
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
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    String jobId =
        sparkQueryDispatcher.cancelJob(
            new AsyncQueryJobMetadata(EMRS_APPLICATION_ID, EMR_JOB_ID, null));
    Assertions.assertEquals(EMR_JOB_ID, jobId);
  }

  @Test
  void testGetQueryResponse() {
    when(emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(new GetJobRunResult().withJobRun(new JobRun().withState(JobRunState.PENDING)));

    // simulate result index is not created yet
    when(jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null))
        .thenReturn(new JSONObject());
    JSONObject result =
        sparkQueryDispatcher.getQueryResponse(
            new AsyncQueryJobMetadata(EMRS_APPLICATION_ID, EMR_JOB_ID, null));
    Assertions.assertEquals("PENDING", result.get("status"));
  }

  @Test
  void testGetQueryResponseWithSuccess() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader,
            flintIndexMetadataReader);
    when(emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(new GetJobRunResult().withJobRun(new JobRun().withState(JobRunState.SUCCESS)));
    JSONObject queryResult = new JSONObject();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put(STATUS_FIELD, "SUCCESS");
    resultMap.put(ERROR_FIELD, "");
    queryResult.put(DATA_FIELD, resultMap);
    when(jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null))
        .thenReturn(queryResult);
    JSONObject result =
        sparkQueryDispatcher.getQueryResponse(
            new AsyncQueryJobMetadata(EMRS_APPLICATION_ID, EMR_JOB_ID, null));
    verify(jobExecutionResponseReader, times(1)).getResultFromOpensearchIndex(EMR_JOB_ID, null);
    Assertions.assertEquals(
        new HashSet<>(Arrays.asList(DATA_FIELD, STATUS_FIELD, ERROR_FIELD)), result.keySet());
    JSONObject dataJson = new JSONObject();
    dataJson.put(ERROR_FIELD, "");
    dataJson.put(STATUS_FIELD, "SUCCESS");
    // JSONObject.similar() compares if two JSON objects are the same, but having perhaps a
    // different order of its attributes.
    // The equals() will compare each string caracter, one-by-one checking if it is the same, having
    // the same order.
    // We need similar.
    Assertions.assertTrue(dataJson.similar(result.get(DATA_FIELD)));
    Assertions.assertEquals("SUCCESS", result.get(STATUS_FIELD));
    verifyNoInteractions(emrServerlessClient);
  }

  @Test
  void testDropIndexQuery() {
    String query = "DROP INDEX size_year ON my_glue.default.http_logs";
    when(flintIndexMetadataReader.getJobIdFromFlintIndexMetadata(
            new IndexDetails(
                "size_year",
                new FullyQualifiedTableName("my_glue.default.http_logs"),
                false,
                true,
                FlintIndexType.COVERING)))
        .thenReturn(EMR_JOB_ID);
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1)).cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID);
    verify(dataSourceUserAuthorizationHelper, times(1)).authorizeDataSource(dataSourceMetadata);
    verify(flintIndexMetadataReader, times(1))
        .getJobIdFromFlintIndexMetadata(
            new IndexDetails(
                "size_year",
                new FullyQualifiedTableName("my_glue.default.http_logs"),
                false,
                true,
                FlintIndexType.COVERING));
    Assertions.assertNotEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertTrue(StringUtils.isAlphanumeric(dispatchQueryResponse.getJobId()));
    Assertions.assertEquals(16, dispatchQueryResponse.getJobId().length());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDropSkippingIndexQuery() {
    String query = "DROP SKIPPING INDEX ON my_glue.default.http_logs";
    when(flintIndexMetadataReader.getJobIdFromFlintIndexMetadata(
            new IndexDetails(
                null,
                new FullyQualifiedTableName("my_glue.default.http_logs"),
                false,
                true,
                FlintIndexType.SKIPPING)))
        .thenReturn(EMR_JOB_ID);
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(1)).cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID);
    verify(dataSourceUserAuthorizationHelper, times(1)).authorizeDataSource(dataSourceMetadata);
    verify(flintIndexMetadataReader, times(1))
        .getJobIdFromFlintIndexMetadata(
            new IndexDetails(
                null,
                new FullyQualifiedTableName("my_glue.default.http_logs"),
                false,
                true,
                FlintIndexType.SKIPPING));
    Assertions.assertNotEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertTrue(StringUtils.isAlphanumeric(dispatchQueryResponse.getJobId()));
    Assertions.assertEquals(16, dispatchQueryResponse.getJobId().length());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDispatchQueryWithExtraSparkSubmitParameters() {
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);

    String extraParameters = "--conf spark.dynamicAllocation.enabled=false";
    DispatchQueryRequest[] requests = {
      constructDispatchQueryRequest( // SQL direct query
          "select * from my_glue.default.http_logs", LangType.SQL, extraParameters),
      constructDispatchQueryRequest( // SQL index query
          "create skipping index on my_glue.default.http_logs (status VALUE_SET)",
          LangType.SQL,
          extraParameters),
      constructDispatchQueryRequest( // PPL query
          "source = my_glue.default.http_logs", LangType.PPL, extraParameters)
    };

    for (DispatchQueryRequest request : requests) {
      when(emrServerlessClient.startJobRun(any())).thenReturn(EMR_JOB_ID);
      sparkQueryDispatcher.dispatch(request);

      // This test is only interested in Spark submit parameters
      verify(emrServerlessClient, times(1))
          .startJobRun(
              argThat(
                  actualReq -> actualReq.getSparkSubmitParams().endsWith(" " + extraParameters)));
      reset(emrServerlessClient);
    }
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
    return " --class org.apache.spark.sql.FlintJob  --conf"
               + " spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider"
               + "  --conf"
               + " spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory"
               + "  --conf"
               + " spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.1.0-SNAPSHOT,org.opensearch:opensearch-spark-sql-application_2.12:0.1.0-SNAPSHOT,org.opensearch:opensearch-spark-ppl_2.12:0.1.0-SNAPSHOT"
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
        + "  --conf"
        + " spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions,org.opensearch.flint.spark.FlintPPLSparkExtensions"
        + "  --conf"
        + " spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        + "  --conf"
        + " spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
        + "  --conf"
        + " spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
        + "  --conf"
        + " spark.hive.metastore.glue.role.arn=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"
        + "  --conf spark.sql.catalog.my_glue=org.opensearch.sql.FlintDelegatingSessionCatalog "
        + authParamConfigBuilder
        + " --conf spark.flint.datasource.name=my_glue ";
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

  private DispatchQueryRequest constructDispatchQueryRequest(
      String query, LangType langType, String extraParameters) {
    return new DispatchQueryRequest(
        EMRS_APPLICATION_ID,
        query,
        "my_glue",
        langType,
        EMRS_EXECUTION_ROLE,
        TEST_CLUSTER_NAME,
        extraParameters);
  }
}
