/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
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
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.MOCK_SESSION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.MOCK_STATEMENT_ID;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.CLUSTER_NAME_TAG_KEY;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.DATASOURCE_TAG_KEY;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.INDEX_TAG_KEY;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.JOB_TYPE_TAG_KEY;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.parameter.DataSourceSparkParameterComposer;
import org.opensearch.sql.spark.parameter.GeneralSparkParameterComposer;
import org.opensearch.sql.spark.parameter.SparkParameterComposerCollection;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class SparkQueryDispatcherTest {

  public static final String MY_GLUE = "my_glue";
  public static final String KEY_FROM_COMPOSER = "key.from.composer";
  public static final String VALUE_FROM_COMPOSER = "value.from.composer";
  public static final String KEY_FROM_DATASOURCE_COMPOSER = "key.from.datasource.composer";
  public static final String VALUE_FROM_DATASOURCE_COMPOSER = "value.from.datasource.composer";
  @Mock private EMRServerlessClient emrServerlessClient;
  @Mock private EMRServerlessClientFactory emrServerlessClientFactory;
  @Mock private DataSourceService dataSourceService;
  @Mock private JobExecutionResponseReader jobExecutionResponseReader;
  @Mock private FlintIndexMetadataService flintIndexMetadataService;
  @Mock private SessionManager sessionManager;
  @Mock private LeaseManager leaseManager;
  @Mock private IndexDMLResultStorageService indexDMLResultStorageService;
  @Mock private FlintIndexOpFactory flintIndexOpFactory;
  @Mock private SparkSubmitParameterModifier sparkSubmitParameterModifier;
  @Mock private QueryIdProvider queryIdProvider;
  @Mock private AsyncQueryRequestContext asyncQueryRequestContext;
  @Mock private MetricsService metricsService;
  private DataSourceSparkParameterComposer dataSourceSparkParameterComposer =
      (datasourceMetadata, sparkSubmitParameters, dispatchQueryRequest, context) -> {
        sparkSubmitParameters.setConfigItem(FLINT_INDEX_STORE_AUTH_KEY, "basic");
        sparkSubmitParameters.setConfigItem(FLINT_INDEX_STORE_HOST_KEY, "HOST");
        sparkSubmitParameters.setConfigItem(FLINT_INDEX_STORE_PORT_KEY, "PORT");
        sparkSubmitParameters.setConfigItem(FLINT_INDEX_STORE_SCHEME_KEY, "SCHEMA");
        sparkSubmitParameters.setConfigItem(
            KEY_FROM_DATASOURCE_COMPOSER, VALUE_FROM_DATASOURCE_COMPOSER);
      };

  private GeneralSparkParameterComposer generalSparkParameterComposer =
      (sparkSubmitParameters, dispatchQueryRequest, context) -> {
        sparkSubmitParameters.setConfigItem(KEY_FROM_COMPOSER, VALUE_FROM_COMPOSER);
      };

  private SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Session session;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Statement statement;

  private SparkQueryDispatcher sparkQueryDispatcher;

  private final String QUERY_ID = "QUERY_ID";

  @Captor ArgumentCaptor<StartJobRequest> startJobRequestArgumentCaptor;

  @BeforeEach
  void setUp() {
    SparkParameterComposerCollection collection = new SparkParameterComposerCollection();
    collection.register(DataSourceType.S3GLUE, dataSourceSparkParameterComposer);
    collection.register(generalSparkParameterComposer);
    sparkSubmitParametersBuilderProvider = new SparkSubmitParametersBuilderProvider(collection);
    QueryHandlerFactory queryHandlerFactory =
        new QueryHandlerFactory(
            jobExecutionResponseReader,
            flintIndexMetadataService,
            sessionManager,
            leaseManager,
            indexDMLResultStorageService,
            flintIndexOpFactory,
            emrServerlessClientFactory,
            metricsService,
            sparkSubmitParametersBuilderProvider);
    sparkQueryDispatcher =
        new SparkQueryDispatcher(
            dataSourceService, sessionManager, queryHandlerFactory, queryIdProvider);
  }

  @Test
  void testDispatchSelectQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "select * from my_glue.default.http_logs";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            DispatchQueryRequest.builder()
                .applicationId(EMRS_APPLICATION_ID)
                .query(query)
                .datasource(MY_GLUE)
                .langType(LangType.SQL)
                .executionRoleARN(EMRS_EXECUTION_ROLE)
                .clusterName(TEST_CLUSTER_NAME)
                .sparkSubmitParameterModifier(sparkSubmitParameterModifier)
                .build(),
            asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchSelectQueryWithLakeFormation() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "select * from my_glue.default.http_logs";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadataWithLakeFormation();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);
    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchSelectQueryWithBasicAuthIndexStoreDatasource() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "select * from my_glue.default.http_logs";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadataWithBasicAuth();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchSelectQueryCreateNewSession() {
    String query = "select * from my_glue.default.http_logs";
    DispatchQueryRequest queryRequest = dispatchQueryRequestWithSessionId(query, null);

    doReturn(true).when(sessionManager).isEnabled();
    doReturn(session).when(sessionManager).createSession(any(), any());
    doReturn(MOCK_SESSION_ID).when(session).getSessionId();
    doReturn(new StatementId(MOCK_STATEMENT_ID)).when(session).submit(any(), any());
    when(session.getSessionModel().getJobId()).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(queryRequest, asyncQueryRequestContext);

    verifyNoInteractions(emrServerlessClient);
    verify(sessionManager, never()).getSession(any(), any());
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
        .getSession(eq(MOCK_SESSION_ID), eq(MY_GLUE));
    doReturn(MOCK_SESSION_ID).when(session).getSessionId();
    doReturn(new StatementId(MOCK_STATEMENT_ID)).when(session).submit(any(), any());
    when(session.getSessionModel().getJobId()).thenReturn(EMR_JOB_ID);
    when(session.isOperationalForDataSource(any())).thenReturn(true);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(queryRequest, asyncQueryRequestContext);

    verifyNoInteractions(emrServerlessClient);
    verify(sessionManager, never()).createSession(any(), any());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    Assertions.assertEquals(MOCK_SESSION_ID, dispatchQueryResponse.getSessionId());
  }

  @Test
  void testDispatchSelectQueryFailedCreateSession() {
    String query = "select * from my_glue.default.http_logs";
    DispatchQueryRequest queryRequest = dispatchQueryRequestWithSessionId(query, null);

    doReturn(true).when(sessionManager).isEnabled();
    doThrow(RuntimeException.class).when(sessionManager).createSession(any(), any());
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> sparkQueryDispatcher.dispatch(queryRequest, asyncQueryRequestContext));

    verifyNoInteractions(emrServerlessClient);
  }

  @Test
  void testDispatchCreateAutoRefreshIndexQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(INDEX_TAG_KEY, "flint_my_glue_default_http_logs_elb_and_requesturi_index");
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    String query =
        "CREATE INDEX elb_and_requestUri ON my_glue.default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query, "streaming");
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:streaming:flint_my_glue_default_http_logs_elb_and_requesturi_index",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchCreateManualRefreshIndexQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, "my_glue");
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query =
        "CREATE INDEX elb_and_requestUri ON my_glue.default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = false)";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            "my_glue", asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchWithPPLQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "source = my_glue.default.http_logs";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            getBaseDispatchQueryRequestBuilder(query).langType(LangType.PPL).build(),
            asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchWithSparkUDFQuery() {
    List<String> udfQueries = new ArrayList<>();
    udfQueries.add(
        "CREATE FUNCTION celsius_to_fahrenheit AS 'org.apache.spark.sql.functions.expr(\"(celsius *"
            + " 9/5) + 32\")'");
    udfQueries.add(
        "CREATE TEMPORARY FUNCTION square AS 'org.apache.spark.sql.functions.expr(\"num * num\")'");
    for (String query : udfQueries) {
      DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
      when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
              MY_GLUE, asyncQueryRequestContext))
          .thenReturn(dataSourceMetadata);

      IllegalArgumentException illegalArgumentException =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  sparkQueryDispatcher.dispatch(
                      getBaseDispatchQueryRequestBuilder(query).langType(LangType.SQL).build(),
                      asyncQueryRequestContext));
      Assertions.assertEquals(
          "Query is not allowed: Creating user-defined functions is not allowed",
          illegalArgumentException.getMessage());
      verifyNoInteractions(emrServerlessClient);
      verifyNoInteractions(flintIndexMetadataService);
    }
  }

  @Test
  void testInvalidSQLQueryDispatchToSpark() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "myselect 1";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(
            DispatchQueryRequest.builder()
                .applicationId(EMRS_APPLICATION_ID)
                .query(query)
                .datasource(MY_GLUE)
                .langType(LangType.SQL)
                .executionRoleARN(EMRS_EXECUTION_ROLE)
                .clusterName(TEST_CLUSTER_NAME)
                .sparkSubmitParameterModifier(sparkSubmitParameterModifier)
                .build(),
            asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchQueryWithoutATableAndDataSourceName() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "show tables";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchIndexQueryWithoutADatasourceName() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(INDEX_TAG_KEY, "flint_my_glue_default_http_logs_elb_and_requesturi_index");
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    String query =
        "CREATE INDEX elb_and_requestUri ON default.http_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query, "streaming");
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:streaming:flint_my_glue_default_http_logs_elb_and_requesturi_index",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchMaterializedViewQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(INDEX_TAG_KEY, "flint_mv_1");
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    String query =
        "CREATE MATERIALIZED VIEW mv_1 AS select * from logs WITH" + " (auto_refresh = true)";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query, "streaming");
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:streaming:flint_mv_1",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchShowMVQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "SHOW MATERIALIZED VIEW IN mys3.default";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testRefreshIndexQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "REFRESH SKIPPING INDEX ON my_glue.default.http_logs";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchDescribeIndexQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, MY_GLUE);
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
    String query = "DESCRIBE SKIPPING INDEX ON mys3.default.http_logs";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query);
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:batch",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            false,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchAlterToAutoRefreshIndexQuery() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(DATASOURCE_TAG_KEY, "my_glue");
    tags.put(INDEX_TAG_KEY, "flint_my_glue_default_http_logs_elb_and_requesturi_index");
    tags.put(CLUSTER_NAME_TAG_KEY, TEST_CLUSTER_NAME);
    tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    String query =
        "ALTER INDEX elb_and_requestUri ON my_glue.default.http_logs WITH"
            + " (auto_refresh = true)";
    String sparkSubmitParameters = constructExpectedSparkSubmitParameterString(query, "streaming");
    StartJobRequest expected =
        new StartJobRequest(
            "TEST_CLUSTER:streaming:flint_my_glue_default_http_logs_elb_and_requesturi_index",
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            sparkSubmitParameters,
            tags,
            true,
            "query_execution_result_my_glue");
    when(emrServerlessClient.startJobRun(expected)).thenReturn(EMR_JOB_ID);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            "my_glue", asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

    DispatchQueryResponse dispatchQueryResponse =
        sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);

    verify(emrServerlessClient, times(1)).startJobRun(startJobRequestArgumentCaptor.capture());
    Assertions.assertEquals(expected, startJobRequestArgumentCaptor.getValue());
    Assertions.assertEquals(EMR_JOB_ID, dispatchQueryResponse.getJobId());
    verifyNoInteractions(flintIndexMetadataService);
  }

  @Test
  void testDispatchAlterToManualRefreshIndexQuery() {
    QueryHandlerFactory queryHandlerFactory = mock(QueryHandlerFactory.class);
    sparkQueryDispatcher =
        new SparkQueryDispatcher(
            dataSourceService, sessionManager, queryHandlerFactory, queryIdProvider);

    String query =
        "ALTER INDEX elb_and_requestUri ON my_glue.default.http_logs WITH"
            + " (auto_refresh = false)";
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            "my_glue", asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);
    when(queryHandlerFactory.getIndexDMLHandler())
        .thenReturn(
            new IndexDMLHandler(
                jobExecutionResponseReader,
                flintIndexMetadataService,
                indexDMLResultStorageService,
                flintIndexOpFactory));

    sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);
    verify(queryHandlerFactory, times(1)).getIndexDMLHandler();
  }

  @Test
  void testDispatchDropIndexQuery() {
    QueryHandlerFactory queryHandlerFactory = mock(QueryHandlerFactory.class);
    sparkQueryDispatcher =
        new SparkQueryDispatcher(
            dataSourceService, sessionManager, queryHandlerFactory, queryIdProvider);

    String query = "DROP INDEX elb_and_requestUri ON my_glue.default.http_logs";
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            "my_glue", asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);
    when(queryHandlerFactory.getIndexDMLHandler())
        .thenReturn(
            new IndexDMLHandler(
                jobExecutionResponseReader,
                flintIndexMetadataService,
                indexDMLResultStorageService,
                flintIndexOpFactory));

    sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);
    verify(queryHandlerFactory, times(1)).getIndexDMLHandler();
  }

  @Test
  void testDispatchVacuumIndexQuery() {
    QueryHandlerFactory queryHandlerFactory = mock(QueryHandlerFactory.class);
    sparkQueryDispatcher =
        new SparkQueryDispatcher(
            dataSourceService, sessionManager, queryHandlerFactory, queryIdProvider);

    String query = "VACUUM INDEX elb_and_requestUri ON my_glue.default.http_logs";
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            "my_glue", asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);
    when(queryHandlerFactory.getIndexDMLHandler())
        .thenReturn(
            new IndexDMLHandler(
                jobExecutionResponseReader,
                flintIndexMetadataService,
                indexDMLResultStorageService,
                flintIndexOpFactory));

    sparkQueryDispatcher.dispatch(getBaseDispatchQueryRequest(query), asyncQueryRequestContext);
    verify(queryHandlerFactory, times(1)).getIndexDMLHandler();
  }

  @Test
  void testDispatchWithUnSupportedDataSourceType() {
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            "my_prometheus", asyncQueryRequestContext))
        .thenReturn(constructPrometheusDataSourceType());
    String query = "select * from my_prometheus.default.http_logs";

    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                sparkQueryDispatcher.dispatch(
                    getBaseDispatchQueryRequestBuilder(query).datasource("my_prometheus").build(),
                    asyncQueryRequestContext));

    Assertions.assertEquals(
        "UnSupported datasource type for async queries:: PROMETHEUS",
        unsupportedOperationException.getMessage());
  }

  @Test
  void testCancelJob() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID, false))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));

    String queryId =
        sparkQueryDispatcher.cancelJob(asyncQueryJobMetadata(), asyncQueryRequestContext);

    Assertions.assertEquals(QUERY_ID, queryId);
  }

  @Test
  void testCancelQueryWithSession() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(MOCK_SESSION_ID, MY_GLUE);
    doReturn(Optional.of(statement)).when(session).get(any());
    doNothing().when(statement).cancel();

    String queryId =
        sparkQueryDispatcher.cancelJob(
            asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, MOCK_SESSION_ID),
            asyncQueryRequestContext);

    verifyNoInteractions(emrServerlessClient);
    verify(statement, times(1)).cancel();
    Assertions.assertEquals(MOCK_STATEMENT_ID, queryId);
  }

  @Test
  void testCancelQueryWithInvalidSession() {
    doReturn(Optional.empty()).when(sessionManager).getSession("invalid", MY_GLUE);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.cancelJob(
                    asyncQueryJobMetadataWithSessionId(MOCK_STATEMENT_ID, "invalid"),
                    asyncQueryRequestContext));

    verifyNoInteractions(emrServerlessClient);
    verifyNoInteractions(session);
    Assertions.assertEquals("no session found. invalid", exception.getMessage());
  }

  @Test
  void testCancelQueryWithInvalidStatementId() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(MOCK_SESSION_ID, MY_GLUE);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sparkQueryDispatcher.cancelJob(
                    asyncQueryJobMetadataWithSessionId("invalid", MOCK_SESSION_ID),
                    asyncQueryRequestContext));

    verifyNoInteractions(emrServerlessClient);
    verifyNoInteractions(statement);
    Assertions.assertEquals(
        "no statement found. " + new StatementId("invalid"), exception.getMessage());
  }

  @Test
  void testCancelQueryWithNoSessionId() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    when(emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID, false))
        .thenReturn(
            new CancelJobRunResult()
                .withJobRunId(EMR_JOB_ID)
                .withApplicationId(EMRS_APPLICATION_ID));

    String queryId =
        sparkQueryDispatcher.cancelJob(asyncQueryJobMetadata(), asyncQueryRequestContext);

    Assertions.assertEquals(QUERY_ID, queryId);
  }

  @Test
  void testGetQueryResponse() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    when(emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(new GetJobRunResult().withJobRun(new JobRun().withState(JobRunState.PENDING)));
    // simulate result index is not created yet
    when(jobExecutionResponseReader.getResultWithJobId(EMR_JOB_ID, null))
        .thenReturn(new JSONObject());

    JSONObject result = sparkQueryDispatcher.getQueryResponse(asyncQueryJobMetadata());

    Assertions.assertEquals("PENDING", result.get("status"));
  }

  @Test
  void testGetQueryResponseWithSession() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(MOCK_SESSION_ID, MY_GLUE);
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
    doReturn(Optional.empty()).when(sessionManager).getSession(MOCK_SESSION_ID, MY_GLUE);
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
    Assertions.assertEquals("no session found. " + MOCK_SESSION_ID, exception.getMessage());
  }

  @Test
  void testGetQueryResponseWithStatementNotExist() {
    doReturn(Optional.of(session)).when(sessionManager).getSession(MOCK_SESSION_ID, MY_GLUE);
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
    JSONObject queryResult = new JSONObject();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put(STATUS_FIELD, "SUCCESS");
    resultMap.put(ERROR_FIELD, "");
    queryResult.put(DATA_FIELD, resultMap);
    when(jobExecutionResponseReader.getResultWithJobId(EMR_JOB_ID, null)).thenReturn(queryResult);

    JSONObject result = sparkQueryDispatcher.getQueryResponse(asyncQueryJobMetadata());

    verify(jobExecutionResponseReader, times(1)).getResultWithJobId(EMR_JOB_ID, null);
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
  void testDispatchQueryWithExtraSparkSubmitParameters() {
    when(emrServerlessClientFactory.getClient(any())).thenReturn(emrServerlessClient);
    DataSourceMetadata dataSourceMetadata = constructMyGlueDataSourceMetadata();
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            MY_GLUE, asyncQueryRequestContext))
        .thenReturn(dataSourceMetadata);

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
      sparkQueryDispatcher.dispatch(request, asyncQueryRequestContext);

      verify(emrServerlessClient, times(1))
          .startJobRun(
              argThat(
                  actualReq -> actualReq.getSparkSubmitParams().endsWith(" " + extraParameters)));
      reset(emrServerlessClient);
    }
  }

  private String constructExpectedSparkSubmitParameterString(String query) {
    return constructExpectedSparkSubmitParameterString(query, null);
  }

  private String constructExpectedSparkSubmitParameterString(String query, String jobType) {
    query = "\"" + query + "\"";
    return " --class org.apache.spark.sql.FlintJob "
        + getConfParam(
            "spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider",
            "spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory",
            "spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
            "spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.3.0-SNAPSHOT,org.opensearch:opensearch-spark-sql-application_2.12:0.3.0-SNAPSHOT,org.opensearch:opensearch-spark-ppl_2.12:0.3.0-SNAPSHOT",
            "spark.jars.repositories=https://aws.oss.sonatype.org/content/repositories/snapshots",
            "spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/",
            "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/",
            "spark.emr-serverless.driverEnv.FLINT_CLUSTER_NAME=TEST_CLUSTER",
            "spark.executorEnv.FLINT_CLUSTER_NAME=TEST_CLUSTER",
            "spark.datasource.flint.host=HOST",
            "spark.datasource.flint.port=PORT",
            "spark.datasource.flint.scheme=SCHEMA",
            "spark.datasource.flint.auth=basic",
            "spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider",
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.opensearch.flint.spark.FlintSparkExtensions,org.opensearch.flint.spark.FlintPPLSparkExtensions",
            "spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog")
        + getConfParam("spark.flint.job.query=" + query)
        + (jobType != null ? getConfParam("spark.flint.job.type=" + jobType) : "")
        + getConfParam(
            KEY_FROM_DATASOURCE_COMPOSER + "=" + VALUE_FROM_DATASOURCE_COMPOSER,
            KEY_FROM_COMPOSER + "=" + VALUE_FROM_COMPOSER);
  }

  private String getConfParam(String... params) {
    return Arrays.stream(params)
        .map(param -> String.format(" --conf %s ", param))
        .collect(Collectors.joining());
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadata() {
    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put(
        "glue.auth.role_arn", "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole");
    properties.put(
        "glue.indexstore.opensearch.uri",
        "https://search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com");
    properties.put("glue.indexstore.opensearch.auth", "awssigv4");
    properties.put("glue.indexstore.opensearch.region", "eu-west-1");
    return new DataSourceMetadata.Builder()
        .setName(MY_GLUE)
        .setConnector(DataSourceType.S3GLUE)
        .setProperties(properties)
        .build();
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadataWithBasicAuth() {
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
    return new DataSourceMetadata.Builder()
        .setName(MY_GLUE)
        .setConnector(DataSourceType.S3GLUE)
        .setProperties(properties)
        .build();
  }

  private DataSourceMetadata constructMyGlueDataSourceMetadataWithLakeFormation() {

    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put(
        "glue.auth.role_arn", "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole");
    properties.put(
        "glue.indexstore.opensearch.uri",
        "https://search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com");
    properties.put("glue.indexstore.opensearch.auth", "awssigv4");
    properties.put("glue.indexstore.opensearch.region", "eu-west-1");
    properties.put("glue.lakeformation.enabled", "true");
    return new DataSourceMetadata.Builder()
        .setName(MY_GLUE)
        .setConnector(DataSourceType.S3GLUE)
        .setProperties(properties)
        .build();
  }

  private DataSourceMetadata constructPrometheusDataSourceType() {
    return new DataSourceMetadata.Builder()
        .setName("my_prometheus")
        .setConnector(DataSourceType.PROMETHEUS)
        .build();
  }

  private DispatchQueryRequest getBaseDispatchQueryRequest(String query) {
    return getBaseDispatchQueryRequestBuilder(query).build();
  }

  private DispatchQueryRequest.DispatchQueryRequestBuilder getBaseDispatchQueryRequestBuilder(
      String query) {
    return DispatchQueryRequest.builder()
        .applicationId(EMRS_APPLICATION_ID)
        .query(query)
        .datasource(MY_GLUE)
        .langType(LangType.SQL)
        .executionRoleARN(EMRS_EXECUTION_ROLE)
        .clusterName(TEST_CLUSTER_NAME)
        .sparkSubmitParameterModifier(sparkSubmitParameterModifier);
  }

  private DispatchQueryRequest constructDispatchQueryRequest(
      String query, LangType langType, String extraParameters) {
    return getBaseDispatchQueryRequestBuilder(query)
        .langType(langType)
        .sparkSubmitParameterModifier((builder) -> builder.extraParameters(extraParameters))
        .build();
  }

  private DispatchQueryRequest dispatchQueryRequestWithSessionId(String query, String sessionId) {
    return getBaseDispatchQueryRequestBuilder(query).sessionId(sessionId).build();
  }

  private AsyncQueryJobMetadata asyncQueryJobMetadata() {
    return AsyncQueryJobMetadata.builder()
        .queryId(QUERY_ID)
        .applicationId(EMRS_APPLICATION_ID)
        .jobId(EMR_JOB_ID)
        .datasourceName(MY_GLUE)
        .build();
  }

  private AsyncQueryJobMetadata asyncQueryJobMetadataWithSessionId(
      String statementId, String sessionId) {
    return AsyncQueryJobMetadata.builder()
        .queryId(statementId)
        .applicationId(EMRS_APPLICATION_ID)
        .jobId(EMR_JOB_ID)
        .sessionId(sessionId)
        .datasourceName(MY_GLUE)
        .build();
  }
}
