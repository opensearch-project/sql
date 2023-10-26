/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.*;
import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_REQUEST_INDEX;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_SESSION_ID;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SESSION_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_REQUEST_BUFFER_INDEX_NAME;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_DOC_TYPE;
import static org.opensearch.sql.spark.execution.statement.StatementModel.SESSION_ID;
import static org.opensearch.sql.spark.execution.statement.StatementModel.STATEMENT_DOC_TYPE;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getSession;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateSessionState;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.datasources.encryptor.EncryptorImpl;
import org.opensearch.sql.datasources.glue.GlueDataSourceFactory;
import org.opensearch.sql.datasources.service.DataSourceMetadataStorage;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.datasources.storage.OpenSearchDataSourceMetadataStorage;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReaderImpl;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.test.OpenSearchIntegTestCase;

public class AsyncQueryExecutorServiceImplSpecTest extends OpenSearchIntegTestCase {
  public static final String DATASOURCE = "mys3";

  private ClusterService clusterService;
  private org.opensearch.sql.common.setting.Settings pluginSettings;
  private NodeClient client;
  private DataSourceServiceImpl dataSourceService;
  private StateStore stateStore;
  private ClusterSettings clusterSettings;

  @Override
  protected Collection<Class<? extends Plugin>> nodePlugins() {
    return Arrays.asList(TestSettingPlugin.class);
  }

  public static class TestSettingPlugin extends Plugin {
    @Override
    public List<Setting<?>> getSettings() {
      return OpenSearchSettings.pluginSettings();
    }
  }

  @Before
  public void setup() {
    clusterService = clusterService();
    client = (NodeClient) cluster().client();
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder()
                .putList(DATASOURCE_URI_HOSTS_DENY_LIST.getKey(), Collections.emptyList())
                .build())
        .get();
    clusterSettings = clusterService.getClusterSettings();
    pluginSettings = new OpenSearchSettings(clusterSettings);
    dataSourceService = createDataSourceService();
    dataSourceService.createDataSource(
        new DataSourceMetadata(
            DATASOURCE,
            Strings.EMPTY,
            DataSourceType.S3GLUE,
            ImmutableList.of(),
            ImmutableMap.of(
                "glue.auth.type",
                "iam_role",
                "glue.auth.role_arn",
                "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                "glue.indexstore.opensearch.uri",
                "http://localhost:9200",
                "glue.indexstore.opensearch.auth",
                "noauth"),
            null));
    stateStore = new StateStore(client, clusterService);
    createIndex(DEFAULT_RESULT_INDEX);
  }

  @After
  public void clean() {
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().putNull(SPARK_EXECUTION_SESSION_ENABLED_SETTING.getKey()).build())
        .get();
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().putNull(SPARK_EXECUTION_SESSION_LIMIT_SETTING.getKey()).build())
        .get();
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().putNull(DATASOURCE_URI_HOSTS_DENY_LIST.getKey()).build())
        .get();
  }

  @Test
  public void withoutSessionCreateAsyncQueryThenGetResultThenCancel() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // disable session
    enableSession(false);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertFalse(clusterService().state().routingTable().hasIndex(SPARK_REQUEST_BUFFER_INDEX_NAME));
    emrsClient.startJobRunCalled(1);

    // 2. fetch async query result.
    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertEquals("RUNNING", asyncQueryResults.getStatus());
    emrsClient.getJobRunResultCalled(1);

    // 3. cancel async query.
    String cancelQueryId = asyncQueryExecutorService.cancelQuery(response.getQueryId());
    assertEquals(response.getQueryId(), cancelQueryId);
    emrsClient.cancelJobRunCalled(1);
  }

  @Test
  public void createAsyncQueryCreateJobWithCorrectParameters() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    enableSession(false);
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    String params = emrsClient.getJobRequest().getSparkSubmitParams();
    assertNull(response.getSessionId());
    assertTrue(params.contains(String.format("--class %s", DEFAULT_CLASS_NAME)));
    assertFalse(
        params.contains(
            String.format("%s=%s", FLINT_JOB_REQUEST_INDEX, SPARK_REQUEST_BUFFER_INDEX_NAME)));
    assertFalse(
        params.contains(String.format("%s=%s", FLINT_JOB_SESSION_ID, response.getSessionId())));

    // enable session
    enableSession(true);
    response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    params = emrsClient.getJobRequest().getSparkSubmitParams();
    assertTrue(params.contains(String.format("--class %s", FLINT_SESSION_CLASS_NAME)));
    assertTrue(
        params.contains(
            String.format("%s=%s", FLINT_JOB_REQUEST_INDEX, SPARK_REQUEST_BUFFER_INDEX_NAME)));
    assertTrue(
        params.contains(String.format("%s=%s", FLINT_JOB_SESSION_ID, response.getSessionId())));
  }

  @Test
  public void withSessionCreateAsyncQueryThenGetResultThenCancel() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getSessionId());
    Optional<StatementModel> statementModel =
        getStatement(stateStore, DATASOURCE).apply(response.getQueryId());
    assertTrue(statementModel.isPresent());
    assertEquals(StatementState.WAITING, statementModel.get().getStatementState());

    // 2. fetch async query result.
    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertTrue(Strings.isEmpty(asyncQueryResults.getError()));
    assertEquals(StatementState.WAITING.getState(), asyncQueryResults.getStatus());

    // 3. cancel async query.
    String cancelQueryId = asyncQueryExecutorService.cancelQuery(response.getQueryId());
    assertEquals(response.getQueryId(), cancelQueryId);
  }

  @Test
  public void reuseSessionWhenCreateAsyncQuery() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());

    // 2. reuse session id
    CreateAsyncQueryResponse second =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", DATASOURCE, LangType.SQL, first.getSessionId()));

    assertEquals(first.getSessionId(), second.getSessionId());
    assertNotEquals(first.getQueryId(), second.getQueryId());
    // one session doc.
    assertEquals(
        1,
        search(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("type", SESSION_DOC_TYPE))
                .must(QueryBuilders.termQuery(SESSION_ID, first.getSessionId()))));
    // two statement docs has same sessionId.
    assertEquals(
        2,
        search(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("type", STATEMENT_DOC_TYPE))
                .must(QueryBuilders.termQuery(SESSION_ID, first.getSessionId()))));

    Optional<StatementModel> firstModel =
        getStatement(stateStore, DATASOURCE).apply(first.getQueryId());
    assertTrue(firstModel.isPresent());
    assertEquals(StatementState.WAITING, firstModel.get().getStatementState());
    assertEquals(first.getQueryId(), firstModel.get().getStatementId().getId());
    assertEquals(first.getQueryId(), firstModel.get().getQueryId());
    Optional<StatementModel> secondModel =
        getStatement(stateStore, DATASOURCE).apply(second.getQueryId());
    assertEquals(StatementState.WAITING, secondModel.get().getStatementState());
    assertEquals(second.getQueryId(), secondModel.get().getStatementId().getId());
    assertEquals(second.getQueryId(), secondModel.get().getQueryId());
  }

  @Test
  public void batchQueryHasTimeout() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    enableSession(false);
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));

    assertEquals(120L, (long) emrsClient.getJobRequest().executionTimeout());
  }

  @Test
  public void interactiveQueryNoTimeout() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    asyncQueryExecutorService.createAsyncQuery(
        new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertEquals(0L, (long) emrsClient.getJobRequest().executionTimeout());
  }

  @Ignore(
      "flaky test, java.lang.IllegalArgumentException: Right now only AES/GCM/NoPadding is"
          + " supported")
  @Test
  public void datasourceWithBasicAuth() {
    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put(
        "glue.auth.role_arn", "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "basicauth");
    properties.put("glue.indexstore.opensearch.auth.username", "username");
    properties.put("glue.indexstore.opensearch.auth.password", "password");

    dataSourceService.createDataSource(
        new DataSourceMetadata(
            "mybasicauth",
            Strings.EMPTY,
            DataSourceType.S3GLUE,
            ImmutableList.of(),
            properties,
            null));
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    asyncQueryExecutorService.createAsyncQuery(
        new CreateAsyncQueryRequest("select 1", "mybasicauth", LangType.SQL, null));
    String params = emrsClient.getJobRequest().getSparkSubmitParams();
    assertTrue(params.contains(String.format("--conf spark.datasource.flint.auth=basic")));
    assertTrue(
        params.contains(String.format("--conf spark.datasource.flint.auth.username=username")));
    assertTrue(
        params.contains(String.format("--conf spark.datasource.flint.auth.password=password")));
  }

  @Test
  public void withSessionCreateAsyncQueryFailed() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("myselect 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getSessionId());
    Optional<StatementModel> statementModel =
        getStatement(stateStore, DATASOURCE).apply(response.getQueryId());
    assertTrue(statementModel.isPresent());
    assertEquals(StatementState.WAITING, statementModel.get().getStatementState());

    // 2. fetch async query result. not result write to SPARK_RESPONSE_BUFFER_INDEX_NAME yet.
    // mock failed statement.
    StatementModel submitted = statementModel.get();
    StatementModel mocked =
        StatementModel.builder()
            .version("1.0")
            .statementState(submitted.getStatementState())
            .statementId(submitted.getStatementId())
            .sessionId(submitted.getSessionId())
            .applicationId(submitted.getApplicationId())
            .jobId(submitted.getJobId())
            .langType(submitted.getLangType())
            .datasourceName(submitted.getDatasourceName())
            .query(submitted.getQuery())
            .queryId(submitted.getQueryId())
            .submitTime(submitted.getSubmitTime())
            .error("mock error")
            .seqNo(submitted.getSeqNo())
            .primaryTerm(submitted.getPrimaryTerm())
            .build();
    updateStatementState(stateStore, DATASOURCE).apply(mocked, StatementState.FAILED);

    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertEquals(StatementState.FAILED.getState(), asyncQueryResults.getStatus());
    assertEquals("mock error", asyncQueryResults.getError());
  }

  // https://github.com/opensearch-project/sql/issues/2344
  @Test
  public void createSessionMoreThanLimitFailed() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);
    // only allow one session in domain.
    setSessionLimit(1);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());
    setSessionState(first.getSessionId(), SessionState.RUNNING);

    // 2. create async query without session.
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null)));
    assertEquals(
        "The maximum number of active sessions can be supported is 1", exception.getMessage());
  }

  // https://github.com/opensearch-project/sql/issues/2360
  @Test
  public void recreateSessionIfNotReady() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());

    // set sessionState to FAIL
    setSessionState(first.getSessionId(), SessionState.FAIL);

    // 2. reuse session id
    CreateAsyncQueryResponse second =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", DATASOURCE, LangType.SQL, first.getSessionId()));

    assertNotEquals(first.getSessionId(), second.getSessionId());

    // set sessionState to FAIL
    setSessionState(second.getSessionId(), SessionState.DEAD);

    // 3. reuse session id
    CreateAsyncQueryResponse third =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", DATASOURCE, LangType.SQL, second.getSessionId()));
    assertNotEquals(second.getSessionId(), third.getSessionId());
  }

  @Test
  public void submitQueryInInvalidSessionWillCreateNewSession() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    // 1. create async query with invalid sessionId
    SessionId invalidSessionId = SessionId.newSessionId(DATASOURCE);
    CreateAsyncQueryResponse asyncQuery =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", DATASOURCE, LangType.SQL, invalidSessionId.getSessionId()));
    assertNotNull(asyncQuery.getSessionId());
    assertNotEquals(invalidSessionId.getSessionId(), asyncQuery.getSessionId());
  }

  @Test
  public void datasourceNameIncludeUppercase() {
    dataSourceService.createDataSource(
        new DataSourceMetadata(
            "TESTS3",
            Strings.EMPTY,
            DataSourceType.S3GLUE,
            ImmutableList.of(),
            ImmutableMap.of(
                "glue.auth.type",
                "iam_role",
                "glue.auth.role_arn",
                "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                "glue.indexstore.opensearch.uri",
                "http://localhost:9200",
                "glue.indexstore.opensearch.auth",
                "noauth"),
            null));

    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // enable session
    enableSession(true);

    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", "TESTS3", LangType.SQL, null));
    String params = emrsClient.getJobRequest().getSparkSubmitParams();

    assertNotNull(response.getSessionId());
    assertTrue(
        params.contains(
            "--conf spark.sql.catalog.TESTS3=org.opensearch.sql.FlintDelegatingSessionCatalog"));
  }

  private DataSourceServiceImpl createDataSourceService() {
    String masterKey = "a57d991d9b573f75b9bba1df";
    DataSourceMetadataStorage dataSourceMetadataStorage =
        new OpenSearchDataSourceMetadataStorage(
            client, clusterService, new EncryptorImpl(masterKey));
    return new DataSourceServiceImpl(
        new ImmutableSet.Builder<DataSourceFactory>()
            .add(new GlueDataSourceFactory(pluginSettings))
            .build(),
        dataSourceMetadataStorage,
        meta -> {});
  }

  private AsyncQueryExecutorService createAsyncQueryExecutorService(
      EMRServerlessClient emrServerlessClient) {
    StateStore stateStore = new StateStore(client, clusterService);
    AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService =
        new OpensearchAsyncQueryJobMetadataStorageService(stateStore);
    JobExecutionResponseReader jobExecutionResponseReader = new JobExecutionResponseReader(client);
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            this.dataSourceService,
            new DataSourceUserAuthorizationHelperImpl(client),
            jobExecutionResponseReader,
            new FlintIndexMetadataReaderImpl(client),
            client,
            new SessionManager(stateStore, emrServerlessClient, pluginSettings));
    return new AsyncQueryExecutorServiceImpl(
        asyncQueryJobMetadataStorageService,
        sparkQueryDispatcher,
        this::sparkExecutionEngineConfig);
  }

  public static class LocalEMRSClient implements EMRServerlessClient {

    private int startJobRunCalled = 0;
    private int cancelJobRunCalled = 0;
    private int getJobResult = 0;

    @Getter private StartJobRequest jobRequest;

    @Override
    public String startJobRun(StartJobRequest startJobRequest) {
      jobRequest = startJobRequest;
      startJobRunCalled++;
      return "jobId";
    }

    @Override
    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
      getJobResult++;
      JobRun jobRun = new JobRun();
      jobRun.setState("RUNNING");
      return new GetJobRunResult().withJobRun(jobRun);
    }

    @Override
    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
      cancelJobRunCalled++;
      return new CancelJobRunResult().withJobRunId(jobId);
    }

    public void startJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, startJobRunCalled);
    }

    public void cancelJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, cancelJobRunCalled);
    }

    public void getJobRunResultCalled(int expectedTimes) {
      assertEquals(expectedTimes, getJobResult);
    }
  }

  public SparkExecutionEngineConfig sparkExecutionEngineConfig() {
    return new SparkExecutionEngineConfig("appId", "us-west-2", "roleArn", "", "myCluster");
  }

  public void enableSession(boolean enabled) {
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder()
                .put(SPARK_EXECUTION_SESSION_ENABLED_SETTING.getKey(), enabled)
                .build())
        .get();
  }

  public void setSessionLimit(long limit) {
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().put(SPARK_EXECUTION_SESSION_LIMIT_SETTING.getKey(), limit).build())
        .get();
  }

  int search(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(DATASOURCE_TO_REQUEST_INDEX.apply(DATASOURCE));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest).actionGet();

    return searchResponse.getHits().getHits().length;
  }

  void setSessionState(String sessionId, SessionState sessionState) {
    Optional<SessionModel> model = getSession(stateStore, DATASOURCE).apply(sessionId);
    SessionModel updated =
        updateSessionState(stateStore, DATASOURCE).apply(model.get(), sessionState);
    assertEquals(sessionState, updated.getSessionState());
  }
}
