/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI;
import static org.opensearch.sql.spark.dispatcher.IndexDMLHandler.DML_QUERY_JOB_ID;
import static org.opensearch.sql.spark.dispatcher.IndexDMLHandler.DROP_INDEX_JOB_ID;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.CancelJobRunRequest;
import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.json.JSONArray;
import org.json.JSONObject;
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
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata.AsyncQueryJobMetadataBuilder;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.EmrServerlessClientImpl;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.dispatcher.QueryHandlerFactory;
import org.opensearch.sql.spark.dispatcher.QueryIdProvider;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.CreateSessionRequest;
import org.opensearch.sql.spark.execution.session.SessionConfigSupplier;
import org.opensearch.sql.spark.execution.session.SessionIdProvider;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;
import org.opensearch.sql.spark.flint.FlintIndexClient;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.parameter.SparkParameterComposerCollection;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

/**
 * This tests async-query-core library end-to-end using mocked implementation of extension points.
 * It intends to cover major happy cases.
 */
@ExtendWith(MockitoExtension.class)
public class AsyncQueryCoreIntegTest {

  public static final String QUERY_ID = "QUERY_ID";
  public static final String SESSION_ID = "SESSION_ID";
  public static final String DATASOURCE_NAME = "DATASOURCE_NAME";
  public static final String INDEX_NAME = "INDEX_NAME";
  public static final String APPLICATION_ID = "APPLICATION_ID";
  public static final String JOB_ID = "JOB_ID";
  public static final String ACCOUNT_ID = "ACCOUNT_ID";
  public static final String RESULT_INDEX = "RESULT_INDEX";
  @Mock SparkSubmitParameterModifier sparkSubmitParameterModifier;
  @Mock SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;
  @Mock SessionConfigSupplier sessionConfigSupplier;
  @Mock LeaseManager leaseManager;
  @Mock JobExecutionResponseReader jobExecutionResponseReader;
  @Mock DataSourceService dataSourceService;
  EMRServerlessClientFactory emrServerlessClientFactory;
  @Mock AWSEMRServerless awsemrServerless;
  @Mock SessionIdProvider sessionIdProvider;
  @Mock QueryIdProvider queryIdProvider;
  @Mock FlintIndexClient flintIndexClient;
  @Mock AsyncQueryRequestContext asyncQueryRequestContext;
  @Mock MetricsService metricsService;
  @Mock SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider;

  // storage services
  @Mock AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;
  @Mock SessionStorageService sessionStorageService;
  @Mock StatementStorageService statementStorageService;
  @Mock FlintIndexMetadataService flintIndexMetadataService;
  @Mock FlintIndexStateModelService flintIndexStateModelService;
  @Mock IndexDMLResultStorageService indexDMLResultStorageService;

  @Captor ArgumentCaptor<DispatchQueryRequest> dispatchQueryRequestArgumentCaptor;
  @Captor ArgumentCaptor<CancelJobRunRequest> cancelJobRunRequestArgumentCaptor;
  @Captor ArgumentCaptor<GetJobRunRequest> getJobRunRequestArgumentCaptor;
  @Captor ArgumentCaptor<IndexDMLResult> indexDMLResultArgumentCaptor;
  @Captor ArgumentCaptor<AsyncQueryJobMetadata> asyncQueryJobMetadataArgumentCaptor;
  @Captor ArgumentCaptor<FlintIndexOptions> flintIndexOptionsArgumentCaptor;
  @Captor ArgumentCaptor<StartJobRunRequest> startJobRunRequestArgumentCaptor;
  @Captor ArgumentCaptor<CreateSessionRequest> createSessionRequestArgumentCaptor;

  AsyncQueryExecutorService asyncQueryExecutorService;

  @BeforeEach
  public void setUp() {
    emrServerlessClientFactory =
        (accountId) -> new EmrServerlessClientImpl(awsemrServerless, metricsService);
    SparkParameterComposerCollection collection = new SparkParameterComposerCollection();
    collection.register(
        DataSourceType.S3GLUE,
        (dataSourceMetadata, sparkSubmitParameters, dispatchQueryRequest, context) ->
            sparkSubmitParameters.setConfigItem(
                "key.from.datasource.composer", "value.from.datasource.composer"));
    collection.register(
        (sparkSubmitParameters, dispatchQueryRequest, context) ->
            sparkSubmitParameters.setConfigItem(
                "key.from.generic.composer", "value.from.generic.composer"));
    SessionManager sessionManager =
        new SessionManager(
            sessionStorageService,
            statementStorageService,
            emrServerlessClientFactory,
            sessionConfigSupplier,
            sessionIdProvider);
    FlintIndexOpFactory flintIndexOpFactory =
        new FlintIndexOpFactory(
            flintIndexStateModelService,
            flintIndexClient,
            flintIndexMetadataService,
            emrServerlessClientFactory);
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
            new SparkSubmitParametersBuilderProvider(collection));
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            dataSourceService, sessionManager, queryHandlerFactory, queryIdProvider);
    asyncQueryExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService,
            sparkQueryDispatcher,
            sparkExecutionEngineConfigSupplier);
  }

  @Test
  public void createDropIndexQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    String indexName = "flint_datasource_name_table_name_index_name_index";
    givenFlintIndexMetadataExists(indexName);
    givenCancelJobRunSucceed();
    givenGetJobRunReturnJobRunWithState("Cancelled");

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "DROP INDEX index_name ON table_name", DATASOURCE_NAME, LangType.SQL),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertNull(response.getSessionId());
    verifyGetQueryIdCalled();
    verifyCancelJobRunCalled();
    verifyCreateIndexDMLResultCalled();
    verifyStoreJobMetadataCalled(DML_QUERY_JOB_ID);
  }

  @Test
  public void createVacuumIndexQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    String indexName = "flint_datasource_name_table_name_index_name_index";
    givenFlintIndexMetadataExists(indexName);

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "VACUUM INDEX index_name ON table_name", DATASOURCE_NAME, LangType.SQL),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertNull(response.getSessionId());
    verifyGetQueryIdCalled();
    verify(flintIndexClient).deleteIndex(indexName);
    verifyCreateIndexDMLResultCalled();
    verifyStoreJobMetadataCalled(DML_QUERY_JOB_ID);
  }

  @Test
  public void createAlterIndexQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    String indexName = "flint_datasource_name_table_name_index_name_index";
    givenFlintIndexMetadataExists(indexName);
    givenCancelJobRunSucceed();
    givenGetJobRunReturnJobRunWithState("Cancelled");

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "ALTER INDEX index_name ON table_name WITH (auto_refresh = false)",
                DATASOURCE_NAME,
                LangType.SQL),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertNull(response.getSessionId());
    verifyGetQueryIdCalled();
    verify(flintIndexMetadataService)
        .updateIndexToManualRefresh(eq(indexName), flintIndexOptionsArgumentCaptor.capture());
    FlintIndexOptions flintIndexOptions = flintIndexOptionsArgumentCaptor.getValue();
    assertFalse(flintIndexOptions.autoRefresh());
    verifyCancelJobRunCalled();
    verifyCreateIndexDMLResultCalled();
    verifyStoreJobMetadataCalled(DML_QUERY_JOB_ID);
  }

  @Test
  public void createStreamingQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    when(awsemrServerless.startJobRun(any()))
        .thenReturn(new StartJobRunResult().withApplicationId(APPLICATION_ID).withJobRunId(JOB_ID));

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "CREATE INDEX index_name ON table_name(l_orderkey, l_quantity)"
                    + " WITH (auto_refresh = true)",
                DATASOURCE_NAME,
                LangType.SQL),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertNull(response.getSessionId());
    verifyGetQueryIdCalled();
    verify(leaseManager).borrow(any());
    verifyStartJobRunCalled();
    verifyStoreJobMetadataCalled(JOB_ID);
  }

  private void verifyStartJobRunCalled() {
    verify(awsemrServerless).startJobRun(startJobRunRequestArgumentCaptor.capture());
    StartJobRunRequest startJobRunRequest = startJobRunRequestArgumentCaptor.getValue();
    assertEquals(APPLICATION_ID, startJobRunRequest.getApplicationId());
    String submitParameters =
        startJobRunRequest.getJobDriver().getSparkSubmit().getSparkSubmitParameters();
    assertTrue(
        submitParameters.contains("key.from.datasource.composer=value.from.datasource.composer"));
    assertTrue(submitParameters.contains("key.from.generic.composer=value.from.generic.composer"));
  }

  @Test
  public void createCreateIndexQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    when(awsemrServerless.startJobRun(any()))
        .thenReturn(new StartJobRunResult().withApplicationId(APPLICATION_ID).withJobRunId(JOB_ID));

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "CREATE INDEX index_name ON table_name(l_orderkey, l_quantity)"
                    + " WITH (auto_refresh = false)",
                DATASOURCE_NAME,
                LangType.SQL),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertNull(response.getSessionId());
    verifyGetQueryIdCalled();
    verifyStartJobRunCalled();
    verifyStoreJobMetadataCalled(JOB_ID);
  }

  @Test
  public void createRefreshQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    when(awsemrServerless.startJobRun(any()))
        .thenReturn(new StartJobRunResult().withApplicationId(APPLICATION_ID).withJobRunId(JOB_ID));

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "REFRESH INDEX index_name ON table_name", DATASOURCE_NAME, LangType.SQL),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertNull(response.getSessionId());
    verifyGetQueryIdCalled();
    verify(leaseManager).borrow(any());
    verifyStartJobRunCalled();
    verifyStoreJobMetadataCalled(JOB_ID);
  }

  @Test
  public void createInteractiveQuery() {
    givenSparkExecutionEngineConfigIsSupplied();
    givenValidDataSourceMetadataExist();
    givenSessionExists();
    when(queryIdProvider.getQueryId(any())).thenReturn(QUERY_ID);
    when(sessionIdProvider.getSessionId(any())).thenReturn(SESSION_ID);
    givenSessionExists(); // called twice
    when(awsemrServerless.startJobRun(any()))
        .thenReturn(new StartJobRunResult().withApplicationId(APPLICATION_ID).withJobRunId(JOB_ID));

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "SELECT * FROM table_name", DATASOURCE_NAME, LangType.SQL, SESSION_ID),
            asyncQueryRequestContext);

    assertEquals(QUERY_ID, response.getQueryId());
    assertEquals(SESSION_ID, response.getSessionId());
    verifyGetQueryIdCalled();
    verifyGetSessionIdCalled();
    verify(leaseManager).borrow(any());
    verifyStartJobRunCalled();
    verifyStoreJobMetadataCalled(JOB_ID);
  }

  @Test
  public void getResultOfInteractiveQuery() {
    givenJobMetadataExists(
        getBaseAsyncQueryJobMetadataBuilder()
            .queryId(QUERY_ID)
            .sessionId(SESSION_ID)
            .resultIndex(RESULT_INDEX));
    JSONObject result = getValidExecutionResponse();
    when(jobExecutionResponseReader.getResultWithQueryId(QUERY_ID, RESULT_INDEX))
        .thenReturn(result);

    AsyncQueryExecutionResponse response = asyncQueryExecutorService.getAsyncQueryResults(QUERY_ID);

    assertEquals("SUCCESS", response.getStatus());
    assertEquals(SESSION_ID, response.getSessionId());
    assertEquals("{col1:\"value\"}", response.getResults().get(0).toString());
  }

  @Test
  public void getResultOfIndexDMLQuery() {
    givenJobMetadataExists(
        getBaseAsyncQueryJobMetadataBuilder()
            .queryId(QUERY_ID)
            .jobId(DROP_INDEX_JOB_ID)
            .resultIndex(RESULT_INDEX));
    JSONObject result = getValidExecutionResponse();
    when(jobExecutionResponseReader.getResultWithQueryId(QUERY_ID, RESULT_INDEX))
        .thenReturn(result);

    AsyncQueryExecutionResponse response = asyncQueryExecutorService.getAsyncQueryResults(QUERY_ID);

    assertEquals("SUCCESS", response.getStatus());
    assertNull(response.getSessionId());
    assertEquals("{col1:\"value\"}", response.getResults().get(0).toString());
  }

  @Test
  public void getResultOfRefreshQuery() {
    givenJobMetadataExists(
        getBaseAsyncQueryJobMetadataBuilder()
            .queryId(QUERY_ID)
            .jobId(JOB_ID)
            .jobType(JobType.BATCH)
            .resultIndex(RESULT_INDEX));
    JSONObject result = getValidExecutionResponse();
    when(jobExecutionResponseReader.getResultWithJobId(JOB_ID, RESULT_INDEX)).thenReturn(result);

    AsyncQueryExecutionResponse response = asyncQueryExecutorService.getAsyncQueryResults(QUERY_ID);

    assertEquals("SUCCESS", response.getStatus());
    assertNull(response.getSessionId());
    assertEquals("{col1:\"value\"}", response.getResults().get(0).toString());
  }

  @Test
  public void cancelInteractiveQuery() {
    givenJobMetadataExists(getBaseAsyncQueryJobMetadataBuilder().sessionId(SESSION_ID));
    givenSessionExists();
    when(sessionConfigSupplier.getSessionInactivityTimeoutMillis()).thenReturn(100000L);
    final StatementModel statementModel = givenStatementExists();
    StatementModel canceledStatementModel =
        StatementModel.copyWithState(statementModel, StatementState.CANCELLED, ImmutableMap.of());
    when(statementStorageService.updateStatementState(statementModel, StatementState.CANCELLED))
        .thenReturn(canceledStatementModel);

    String result = asyncQueryExecutorService.cancelQuery(QUERY_ID);

    assertEquals(QUERY_ID, result);
    verify(statementStorageService).updateStatementState(statementModel, StatementState.CANCELLED);
  }

  @Test
  public void cancelIndexDMLQuery() {
    givenJobMetadataExists(getBaseAsyncQueryJobMetadataBuilder().jobId(DROP_INDEX_JOB_ID));

    assertThrows(
        IllegalArgumentException.class, () -> asyncQueryExecutorService.cancelQuery(QUERY_ID));
  }

  @Test
  public void cancelRefreshQuery() {
    givenJobMetadataExists(
        getBaseAsyncQueryJobMetadataBuilder().jobType(JobType.BATCH).indexName(INDEX_NAME));
    when(flintIndexMetadataService.getFlintIndexMetadata(INDEX_NAME))
        .thenReturn(
            ImmutableMap.of(
                INDEX_NAME,
                FlintIndexMetadata.builder()
                    .latestId(null)
                    .appId(APPLICATION_ID)
                    .jobId(JOB_ID)
                    .build()));
    givenCancelJobRunSucceed();
    when(awsemrServerless.getJobRun(any()))
        .thenReturn(
            new GetJobRunResult()
                .withJobRun(new JobRun().withJobRunId(JOB_ID).withState("Cancelled")));

    String result = asyncQueryExecutorService.cancelQuery(QUERY_ID);

    assertEquals(QUERY_ID, result);
    verifyCancelJobRunCalled();
    verifyGetJobRunRequest();
  }

  @Test
  public void cancelStreamingQuery() {
    givenJobMetadataExists(getBaseAsyncQueryJobMetadataBuilder().jobType(JobType.STREAMING));

    assertThrows(
        IllegalArgumentException.class, () -> asyncQueryExecutorService.cancelQuery(QUERY_ID));
  }

  @Test
  public void cancelBatchQuery() {
    givenJobMetadataExists(getBaseAsyncQueryJobMetadataBuilder().jobId(JOB_ID));
    givenCancelJobRunSucceed();

    String result = asyncQueryExecutorService.cancelQuery(QUERY_ID);

    assertEquals(QUERY_ID, result);
    verifyCancelJobRunCalled();
  }

  private void givenSparkExecutionEngineConfigIsSupplied() {
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(asyncQueryRequestContext))
        .thenReturn(
            SparkExecutionEngineConfig.builder()
                .applicationId(APPLICATION_ID)
                .accountId(ACCOUNT_ID)
                .sparkSubmitParameterModifier(sparkSubmitParameterModifier)
                .build());
  }

  private void givenFlintIndexMetadataExists(String indexName) {
    when(flintIndexMetadataService.getFlintIndexMetadata(indexName))
        .thenReturn(
            ImmutableMap.of(
                indexName,
                FlintIndexMetadata.builder()
                    .appId(APPLICATION_ID)
                    .jobId(JOB_ID)
                    .opensearchIndexName(indexName)
                    .build()));
  }

  private void givenValidDataSourceMetadataExist() {
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(DATASOURCE_NAME))
        .thenReturn(
            new DataSourceMetadata.Builder()
                .setName(DATASOURCE_NAME)
                .setConnector(DataSourceType.S3GLUE)
                .setProperties(
                    ImmutableMap.<String, String>builder()
                        .put(GLUE_INDEX_STORE_OPENSEARCH_URI, "https://open.search.cluster:9200/")
                        .put(GLUE_INDEX_STORE_OPENSEARCH_AUTH, AuthenticationType.NOAUTH.getName())
                        .build())
                .build());
  }

  private void givenGetJobRunReturnJobRunWithState(String state) {
    when(awsemrServerless.getJobRun(any()))
        .thenReturn(
            new GetJobRunResult()
                .withJobRun(
                    new JobRun()
                        .withJobRunId(JOB_ID)
                        .withApplicationId(APPLICATION_ID)
                        .withState(state)));
  }

  private void verifyGetQueryIdCalled() {
    verify(queryIdProvider).getQueryId(dispatchQueryRequestArgumentCaptor.capture());
    DispatchQueryRequest dispatchQueryRequest = dispatchQueryRequestArgumentCaptor.getValue();
    assertEquals(ACCOUNT_ID, dispatchQueryRequest.getAccountId());
    assertEquals(APPLICATION_ID, dispatchQueryRequest.getApplicationId());
  }

  private void verifyGetSessionIdCalled() {
    verify(sessionIdProvider).getSessionId(createSessionRequestArgumentCaptor.capture());
    CreateSessionRequest createSessionRequest = createSessionRequestArgumentCaptor.getValue();
    assertEquals(ACCOUNT_ID, createSessionRequest.getAccountId());
    assertEquals(APPLICATION_ID, createSessionRequest.getApplicationId());
  }

  private void verifyStoreJobMetadataCalled(String jobId) {
    verify(asyncQueryJobMetadataStorageService)
        .storeJobMetadata(
            asyncQueryJobMetadataArgumentCaptor.capture(), eq(asyncQueryRequestContext));
    AsyncQueryJobMetadata asyncQueryJobMetadata = asyncQueryJobMetadataArgumentCaptor.getValue();
    assertEquals(QUERY_ID, asyncQueryJobMetadata.getQueryId());
    assertEquals(jobId, asyncQueryJobMetadata.getJobId());
    assertEquals(DATASOURCE_NAME, asyncQueryJobMetadata.getDatasourceName());
  }

  private void verifyCreateIndexDMLResultCalled() {
    verify(indexDMLResultStorageService)
        .createIndexDMLResult(indexDMLResultArgumentCaptor.capture(), eq(asyncQueryRequestContext));
    IndexDMLResult indexDMLResult = indexDMLResultArgumentCaptor.getValue();
    assertEquals(QUERY_ID, indexDMLResult.getQueryId());
    assertEquals(DATASOURCE_NAME, indexDMLResult.getDatasourceName());
    assertEquals("SUCCESS", indexDMLResult.getStatus());
    assertEquals("", indexDMLResult.getError());
  }

  private void verifyCancelJobRunCalled() {
    verify(awsemrServerless).cancelJobRun(cancelJobRunRequestArgumentCaptor.capture());
    CancelJobRunRequest cancelJobRunRequest = cancelJobRunRequestArgumentCaptor.getValue();
    assertEquals(JOB_ID, cancelJobRunRequest.getJobRunId());
    assertEquals(APPLICATION_ID, cancelJobRunRequest.getApplicationId());
  }

  private void verifyGetJobRunRequest() {
    verify(awsemrServerless).getJobRun(getJobRunRequestArgumentCaptor.capture());
    GetJobRunRequest getJobRunRequest = getJobRunRequestArgumentCaptor.getValue();
    assertEquals(APPLICATION_ID, getJobRunRequest.getApplicationId());
    assertEquals(JOB_ID, getJobRunRequest.getJobRunId());
  }

  private StatementModel givenStatementExists() {
    StatementModel statementModel =
        StatementModel.builder()
            .queryId(QUERY_ID)
            .statementId(new StatementId(QUERY_ID))
            .statementState(StatementState.RUNNING)
            .build();
    when(statementStorageService.getStatement(QUERY_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(statementModel));
    return statementModel;
  }

  private void givenSessionExists() {
    when(sessionStorageService.getSession(SESSION_ID, DATASOURCE_NAME))
        .thenReturn(
            Optional.of(
                SessionModel.builder()
                    .sessionId(SESSION_ID)
                    .datasourceName(DATASOURCE_NAME)
                    .jobId(JOB_ID)
                    .sessionState(SessionState.RUNNING)
                    .build()));
  }

  private AsyncQueryJobMetadataBuilder getBaseAsyncQueryJobMetadataBuilder() {
    return AsyncQueryJobMetadata.builder()
        .applicationId(APPLICATION_ID)
        .queryId(QUERY_ID)
        .datasourceName(DATASOURCE_NAME);
  }

  private void givenJobMetadataExists(AsyncQueryJobMetadataBuilder metadataBuilder) {
    AsyncQueryJobMetadata metadata = metadataBuilder.build();
    when(asyncQueryJobMetadataStorageService.getJobMetadata(metadata.getQueryId()))
        .thenReturn(Optional.of(metadata));
  }

  private void givenCancelJobRunSucceed() {
    when(awsemrServerless.cancelJobRun(any()))
        .thenReturn(
            new CancelJobRunResult().withJobRunId(JOB_ID).withApplicationId(APPLICATION_ID));
  }

  private static JSONObject getValidExecutionResponse() {
    return new JSONObject()
        .put(
            "data",
            new JSONObject()
                .put("status", "SUCCESS")
                .put(
                    "schema",
                    new JSONArray()
                        .put(
                            new JSONObject().put("column_name", "col1").put("data_type", "string")))
                .put("result", new JSONArray().put("{'col1': 'value'}")));
  }
}
