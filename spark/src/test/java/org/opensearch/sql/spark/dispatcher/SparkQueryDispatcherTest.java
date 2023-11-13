/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.asyncquery.OpensearchAsyncQueryAsyncQueryJobMetadataStorageServiceTest.DS_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.MOCK_SESSION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.MOCK_STATEMENT_ID;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.*;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
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

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Client openSearchClient;

  @Mock private FlintIndexMetadata flintIndexMetadata;

  @Mock private SessionManager sessionManager;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Session session;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Statement statement;

  private SparkQueryDispatcher sparkQueryDispatcher;

  private final AsyncQueryId QUERY_ID = AsyncQueryId.newAsyncQueryId(DS_NAME);

  @Captor ArgumentCaptor<StartJobRequest> startJobRequestArgumentCaptor;

  @BeforeEach
  void setUp() {
    sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader,
            flintIndexMetadataReader,
            openSearchClient,
            sessionManager);
  }

  @Test
  void testDispatchSelectQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.BATCH.getText());
    String query = "select * from my_glue.default.http_logs";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "sigv4",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:non-index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchSelectQueryWithBasicAuthIndexStoreDatasource() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.BATCH.getText());
    String query = "select * from my_glue.default.http_logs";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "basic",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AUTH_USERNAME, "username");
                put(FLINT_INDEX_STORE_AUTH_PASSWORD, "password");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:non-index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchSelectQueryWithNoAuthIndexStoreDatasource() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.BATCH.getText());
    String query = "select * from my_glue.default.http_logs";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "noauth",
            new HashMap<>() {
              {
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:non-index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchSelectQueryCreateNewSession() {
    String query = "select * from my_glue.default.http_logs";
    DispatchQueryRequest queryRequest = dispatchQueryRequestWithSessionId(query, null);

    doReturn(true).when(sessionManager).isEnabled();
    doReturn(session).when(sessionManager).createSession(any());
    doReturn(new SessionId(MOCK_SESSION_ID)).when(session).getSessionId();
    doReturn(new StatementId(MOCK_STATEMENT_ID)).when(session).submit(any());
    when(session.getSessionModel().getJobId()).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse = sparkQueryDispatcher.dispatch(queryRequest);

    verifyNoInteractions(emrServerlessClient);
    verify(sessionManager, never()).getSession(any());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertEquals(MOCK_SESSION_ID, dispatchQueryResponse.getSessionId());
  }

  @Test
  void testDispatchSelectQueryReuseSession() {
    String query = "select * from my_glue.default.http_logs";
    DispatchQueryRequest queryRequest = dispatchQueryRequestWithSessionId(query, MOCK_SESSION_ID);

    doReturn(true).when(sessionManager).isEnabled();
    doReturn(Optional.of(session))
        .when(sessionManager)
        .getSession(eq(new SessionId(MOCK_SESSION_ID)));
    doReturn(new SessionId(MOCK_SESSION_ID)).when(session).getSessionId();
    doReturn(new StatementId(MOCK_STATEMENT_ID)).when(session).submit(any());
    when(session.getSessionModel().getJobId()).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    DispatchQueryResponse dispatchQueryResponse = sparkQueryDispatcher.dispatch(queryRequest);

    verifyNoInteractions(emrServerlessClient);
    verify(sessionManager, never()).createSession(any());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertEquals(MOCK_SESSION_ID, dispatchQueryResponse.getSessionId());
  }

  @Test
  void testDispatchSelectQueryInvalidSession() {
    String query = "select * from my_glue.default.http_logs";
    DispatchQueryRequest queryRequest = dispatchQueryRequestWithSessionId(query, "invalid");

    doReturn(true).when(sessionManager).isEnabled();
    doReturn(Optional.empty()).when(sessionManager).getSession(any());
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> sparkQueryDispatcher.dispatch(queryRequest));

    verifyNoInteractions(emrServerlessClient);
    verify(sessionManager, never()).createSession(any());
    Assertions.assertEquals(
        "no session found. " + new SessionId("invalid"), exception.getMessage());
  }

  @Test
  void testDispatchSelectQueryFailedCreateSession() {
    String query = "select * from my_glue.default.http_logs";
    DispatchQueryRequest queryRequest = dispatchQueryRequestWithSessionId(query, null);

    doReturn(true).when(sessionManager).isEnabled();
    doThrow(RuntimeException.class).when(sessionManager).createSession(any());
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    Assertions.assertThrows(
        RuntimeException.class, () -> sparkQueryDispatcher.dispatch(queryRequest));

    verifyNoInteractions(emrServerlessClient);
  }

  @Test
  void testDispatchIndexQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("index", "flint_my_glue_default_http_logs_elb_and_requesturi_index");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.STREAMING.getText());
    String query =
        "CREATE INDEX elb_and_requestUri ON my_glue.default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    String sparkSubmitParameters =
        withStructuredStreaming(
            constructExpectedSparkSubmitParameterString(
                "sigv4",
                new HashMap<>() {
                  {
                    put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                  }
                }));
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchWithPPLQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.BATCH.getText());
    String query = "source = my_glue.default.http_logs";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "sigv4",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:non-index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchQueryWithoutATableAndDataSourceName() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.BATCH.getText());
    String query = "show tables";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "sigv4",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:non-index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:non-index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchIndexQueryWithoutADatasourceName() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("index", "flint_my_glue_default_http_logs_elb_and_requesturi_index");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.STREAMING.getText());
    String query =
        "CREATE INDEX elb_and_requestUri ON default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    String sparkSubmitParameters =
        withStructuredStreaming(
            constructExpectedSparkSubmitParameterString(
                "sigv4",
                new HashMap<>() {
                  {
                    put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                  }
                }));
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchMaterializedViewQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("index", "flint_mv_1");
    tags.put("cluster", TEST_CLUSTER_NAME);
    tags.put("type", JobType.STREAMING.getText());
    String query =
        "CREATE MATERIALIZED VIEW mv_1 AS query=select * from my_glue.default.logs WITH"
            + " (auto_refresh = true)";
    String sparkSubmitParameters =
        withStructuredStreaming(
            constructExpectedSparkSubmitParameterString(
                "sigv4",
                new HashMap<>() {
                  {
                    put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
                  }
                }));
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchShowMVQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    String query = "SHOW MATERIALIZED VIEW IN mys3.default";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "sigv4",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testRefreshIndexQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    String query = "REFRESH SKIPPING INDEX ON my_glue.default.http_logs";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "sigv4",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertFalse(dispatchQueryResponse.isDropIndexQuery());
    verifyNoInteractions(flintIndexMetadataReader);
  }

  @Test
  void testDispatchDescribeIndexQuery() {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("datasource", "my_glue");
    tags.put("cluster", TEST_CLUSTER_NAME);
    String query = "DESCRIBE SKIPPING INDEX ON mys3.default.http_logs";
    String sparkSubmitParameters =
        constructExpectedSparkSubmitParameterString(
            "sigv4",
            new HashMap<>() {
              {
                put(FLINT_INDEX_STORE_AWSREGION_KEY, "eu-west-1");
              }
            });
    when(emrServerlessClient.startJobRun(
            new StartJobRequest(
                query,
                "TEST_CLUSTER:index-query",
                EMRS_APPLICATION_ID,
                EMRS_EXECUTION_ROLE,
                sparkSubmitParameters,
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
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    StartJobRequest expected =
        new StartJobRequest(
            query,
            "TEST_CLUSTER:index-query",
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            null);
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
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
    String queryId = sparkQueryDispatcher.cancelJob(asyncQueryJobMetadata());
    Assertions.assertEquals(QUERY_ID.getId(), queryId);
  }

  @Test
  void testCancelQueryWithSession() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(new SessionId(MOCK_SESSION_ID));
    doReturn(Optional.of(statement)).when(session).get(any());
    doNothing().when(statement).cancel();

    String queryId =
        sparkQueryDispatcher.cancelJob(
            asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, MOCK_SESSION_ID));

    verifyNoInteractions(emrServerlessClient);
    verify(statement, times(1)).cancel();
    Assertions.assertEquals(MOCK_STATEMENT_ID, queryId);
  }

  @Test
  void testCancelQueryWithInvalidSession() {
    doReturn(Optional.empty()).when(sessionManager).getSession(new SessionId("invalid"));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.cancelJob(
                    asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, "invalid")));

    verifyNoInteractions(emrServerlessClient);
    verifyNoInteractions(session);
    Assertions.assertEquals(
        "no session found. " + new SessionId("invalid"), exception.getMessage());
  }

  @Test
  void testCancelQueryWithInvalidStatementId() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(new SessionId(MOCK_SESSION_ID));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.cancelJob(
                    asyncQueryJobMetadataWithSessionId("invalid", MOCK_SESSION_ID)));

    verifyNoInteractions(emrServerlessClient);
    verifyNoInteractions(statement);
    Assertions.assertEquals(
        "no statement found. " + new StatementId("invalid"), exception.getMessage());
  }

  @Test
  void testCancelQueryWithNoSessionId() {
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    String queryId = sparkQueryDispatcher.cancelJob(asyncQueryJobMetadata());
    Assertions.assertEquals(QUERY_ID.getId(), queryId);
  }

  @Test
  void testGetQueryResponse() {
    when(emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(new GetJobRunResult().withJobRun(new JobRun().withState(JobRunState.PENDING)));

    // simulate result index is not created yet
    when(jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null))
        .thenReturn(new JSONObject());
    JSONObject result = sparkQueryDispatcher.getQueryResponse(asyncQueryJobMetadata());
    Assertions.assertEquals("PENDING", result.get("status"));
  }

  @Test
  void testGetQueryResponseWithSession() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(new SessionId(MOCK_SESSION_ID));
    doReturn(Optional.of(statement)).when(session).get(any());
    when(statement.getStatementModel().getError()).thenReturn("mock error");
    doReturn(StatementState.WAITING).when(statement).getStatementState();

    doReturn(new JSONObject())
        .when(jobExecutionResponseReader)
        .getResultWithQueryId(eq(MOCK_STATEMENT_ID), any());
    JSONObject result =
        sparkQueryDispatcher.getQueryResponse(
            asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, MOCK_SESSION_ID));

    verifyNoInteractions(emrServerlessClient);
    Assertions.assertEquals("waiting", result.get("status"));
  }

  @Test
  void testGetQueryResponseWithInvalidSession() {
    doReturn(Optional.empty()).when(sessionManager).getSession(eq(new SessionId(MOCK_SESSION_ID)));
    doReturn(new JSONObject())
        .when(jobExecutionResponseReader)
        .getResultWithQueryId(eq(MOCK_STATEMENT_ID), any());
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.getQueryResponse(
                    asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, MOCK_SESSION_ID)));

    verifyNoInteractions(emrServerlessClient);
    Assertions.assertEquals(
        "no session found. " + new SessionId(MOCK_SESSION_ID), exception.getMessage());
  }

  @Test
  void testGetQueryResponseWithStatementNotExist() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(new SessionId(MOCK_SESSION_ID));
    doReturn(Optional.empty()).when(session).get(any());
    doReturn(new JSONObject())
        .when(jobExecutionResponseReader)
        .getResultWithQueryId(eq(MOCK_STATEMENT_ID), any());

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.getQueryResponse(
                    asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, MOCK_SESSION_ID)));
    verifyNoInteractions(emrServerlessClient);
    Assertions.assertEquals(
        "no statement found. " + new StatementId(MOCK_STATEMENT_ID), exception.getMessage());
  }

  @Test
  void testGetQueryResponseWithSuccess() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader,
            flintIndexMetadataReader,
            openSearchClient,
            sessionManager);
    JSONObject queryResult = new JSONObject();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put(STATUS_FIELD, "SUCCESS");
    resultMap.put(ERROR_FIELD, "");
    queryResult.put(DATA_FIELD, resultMap);
    when(jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null))
        .thenReturn(queryResult);
    JSONObject result = sparkQueryDispatcher.getQueryResponse(asyncQueryJobMetadata());
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

  // todo. refactor query process logic in plugin.
  @Test
  void testGetQueryResponseOfDropIndex() {
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            dataSourceService,
            dataSourceUserAuthorizationHelper,
            jobExecutionResponseReader,
            flintIndexMetadataReader,
            openSearchClient,
            sessionManager);

    String jobId =
        new SparkQueryDispatcher.DropIndexResult(JobRunState.SUCCESS.toString()).toJobId();

    JSONObject result =
        sparkQueryDispatcher.getQueryResponse(
            new AsyncQueryJobMetadata(
                AsyncQueryId.newAsyncQueryId(DS_NAME),
                EMRS_APPLICATION_ID,
                jobId,
                true,
                null,
                null));
    verify(jobExecutionResponseReader, times(0))
        .getResultFromOpensearchIndex(anyString(), anyString());
    Assertions.assertEquals("SUCCESS", result.get(STATUS_FIELD));
  }

  @Test
  void testDropIndexQuery() throws ExecutionException, InterruptedException {
    String query = "DROP INDEX size_year ON my_glue.default.http_logs";
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .indexName("size_year")
            .fullyQualifiedTableName(new FullyQualifiedTableName("my_glue.default.http_logs"))
            .autoRefresh(false)
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.COVERING)
            .build();
    when(flintIndexMetadataReader.getFlintIndexMetadata(indexQueryDetails))
        .thenReturn(flintIndexMetadata);
    when(flintIndexMetadata.getJobId()).thenReturn(EMR_JOB_ID);
    // auto_refresh == true
    when(flintIndexMetadata.isAutoRefresh()).thenReturn(true);

    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);

    AcknowledgedResponse acknowledgedResponse = mock(AcknowledgedResponse.class);
    when(openSearchClient.admin().indices().delete(any()).get()).thenReturn(acknowledgedResponse);
    when(acknowledgedResponse.isAcknowledged()).thenReturn(true);
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
    verify(flintIndexMetadataReader, times(1)).getFlintIndexMetadata(indexQueryDetails);
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(dispatchQueryResponse.getJobId());
    Assertions.assertEquals(JobRunState.SUCCESS.toString(), dropIndexResult.getStatus());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDropSkippingIndexQuery() throws ExecutionException, InterruptedException {
    String query = "DROP SKIPPING INDEX ON my_glue.default.http_logs";
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .fullyQualifiedTableName(new FullyQualifiedTableName("my_glue.default.http_logs"))
            .autoRefresh(false)
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.SKIPPING)
            .build();
    when(flintIndexMetadataReader.getFlintIndexMetadata(indexQueryDetails))
        .thenReturn(flintIndexMetadata);
    when(flintIndexMetadata.getJobId()).thenReturn(EMR_JOB_ID);
    when(flintIndexMetadata.isAutoRefresh()).thenReturn(true);

    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    AcknowledgedResponse acknowledgedResponse = mock(AcknowledgedResponse.class);
    when(openSearchClient.admin().indices().delete(any()).get()).thenReturn(acknowledgedResponse);

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
    verify(flintIndexMetadataReader, times(1)).getFlintIndexMetadata(indexQueryDetails);
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(dispatchQueryResponse.getJobId());
    Assertions.assertEquals(JobRunState.SUCCESS.toString(), dropIndexResult.getStatus());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDropSkippingIndexQueryAutoRefreshFalse()
      throws ExecutionException, InterruptedException {
    String query = "DROP SKIPPING INDEX ON my_glue.default.http_logs";
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .fullyQualifiedTableName(new FullyQualifiedTableName("my_glue.default.http_logs"))
            .autoRefresh(false)
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.SKIPPING)
            .build();
    when(flintIndexMetadataReader.getFlintIndexMetadata(indexQueryDetails))
        .thenReturn(flintIndexMetadata);
    when(flintIndexMetadata.isAutoRefresh()).thenReturn(false);

    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    AcknowledgedResponse acknowledgedResponse = mock(AcknowledgedResponse.class);
    when(openSearchClient.admin().indices().delete(any()).get()).thenReturn(acknowledgedResponse);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(0)).cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID);
    verify(dataSourceUserAuthorizationHelper, times(1)).authorizeDataSource(dataSourceMetadata);
    verify(flintIndexMetadataReader, times(1)).getFlintIndexMetadata(indexQueryDetails);
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(dispatchQueryResponse.getJobId());
    Assertions.assertEquals(JobRunState.SUCCESS.toString(), dropIndexResult.getStatus());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDropSkippingIndexQueryDeleteIndexException()
      throws ExecutionException, InterruptedException {
    String query = "DROP SKIPPING INDEX ON my_glue.default.http_logs";
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .fullyQualifiedTableName(new FullyQualifiedTableName("my_glue.default.http_logs"))
            .autoRefresh(false)
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.SKIPPING)
            .build();
    when(flintIndexMetadataReader.getFlintIndexMetadata(indexQueryDetails))
        .thenReturn(flintIndexMetadata);
    when(flintIndexMetadata.isAutoRefresh()).thenReturn(false);

    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);

    when(openSearchClient.admin().indices().delete(any()).get())
        .thenThrow(ExecutionException.class);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                EMRS_APPLICATION_ID,
                query,
                "my_glue",
                LangType.SQL,
                EMRS_EXECUTION_ROLE,
                TEST_CLUSTER_NAME));
    verify(emrServerlessClient, times(0)).cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID);
    verify(dataSourceUserAuthorizationHelper, times(1)).authorizeDataSource(dataSourceMetadata);
    verify(flintIndexMetadataReader, times(1)).getFlintIndexMetadata(indexQueryDetails);
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(dispatchQueryResponse.getJobId());
    Assertions.assertEquals(JobRunState.FAILED.toString(), dropIndexResult.getStatus());
    Assertions.assertEquals(
        "{\"error\":\"failed to drop index\",\"status\":\"FAILED\"}",
        dropIndexResult.result().toString());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDropMVQuery() throws ExecutionException, InterruptedException {
    String query = "DROP MATERIALIZED VIEW mv_1";
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .mvName("mv_1")
            .indexQueryActionType(IndexQueryActionType.DROP)
            .fullyQualifiedTableName(null)
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build();
    when(flintIndexMetadataReader.getFlintIndexMetadata(indexQueryDetails))
        .thenReturn(flintIndexMetadata);
    when(flintIndexMetadata.getJobId()).thenReturn(EMR_JOB_ID);
    // auto_refresh == true
    when(flintIndexMetadata.isAutoRefresh()).thenReturn(true);

    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);

    AcknowledgedResponse acknowledgedResponse = mock(AcknowledgedResponse.class);
    when(openSearchClient.admin().indices().delete(any()).get()).thenReturn(acknowledgedResponse);
    when(acknowledgedResponse.isAcknowledged()).thenReturn(true);
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
    verify(flintIndexMetadataReader, times(1)).getFlintIndexMetadata(indexQueryDetails);
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(dispatchQueryResponse.getJobId());
    Assertions.assertEquals(JobRunState.SUCCESS.toString(), dropIndexResult.getStatus());
    Assertions.assertTrue(dispatchQueryResponse.isDropIndexQuery());
  }

  @Test
  void testDispatchQueryWithExtraSparkSubmitParameters() {
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.getRawDataSourceMetadata("my_glue")).thenReturn(dataSourceMetadata);
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);

    String extraParameters = "--conf spark.dynamicAllocation.enabled=false";
    DispatchQueryRequest[] requests = {
      // SQL direct query
      constructDispatchQueryRequest(
          "select * from my_glue.default.http_logs", LangType.SQL, extraParameters),
      // SQL index query
      constructDispatchQueryRequest(
          "create skipping index on my_glue.default.http_logs (status VALUE_SET)",
          LangType.SQL,
          extraParameters),
      // PPL query
      constructDispatchQueryRequest(
          "source = my_glue.default.http_logs", LangType.PPL, extraParameters)
    };

    for (DispatchQueryRequest request : requests) {
      when(emrServerlessClient.startJobRun(any())).thenReturn(EMR_JOB_ID);
      sparkQueryDispatcher.dispatch(request);

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
        + " --conf spark.flint.datasource.name=my_glue "
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

  private DispatchQueryRequest constructDispatchQueryRequest(
      String query, LangType langType, String extraParameters) {
    return new DispatchQueryRequest(
        EMRS_APPLICATION_ID,
        query,
        "my_glue",
        langType,
        EMRS_EXECUTION_ROLE,
        TEST_CLUSTER_NAME,
        extraParameters,
        null);
  }

  private DispatchQueryRequest dispatchQueryRequestWithSessionId(String query, String sessionId) {
    return new DispatchQueryRequest(
        EMRS_APPLICATION_ID,
        query,
        "my_glue",
        LangType.SQL,
        EMRS_EXECUTION_ROLE,
        TEST_CLUSTER_NAME,
        null,
        sessionId);
  }

  private AsyncQueryJobMetadata asyncQueryJobMetadata() {
    return new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null);
  }

  private AsyncQueryJobMetadata asyncQueryJobMetadataWithSessionId(
      String statementId, String sessionId) {
    return new AsyncQueryJobMetadata(
        new AsyncQueryId(statementId), EMRS_APPLICATION_ID, EMR_JOB_ID, false, null, sessionId);
  }
}
