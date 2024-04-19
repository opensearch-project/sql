/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_REQUEST_INDEX;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_SESSION_ID;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SESSION_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_REQUEST_BUFFER_INDEX_NAME;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_DOC_TYPE;
import static org.opensearch.sql.spark.execution.statement.StatementModel.SESSION_ID;
import static org.opensearch.sql.spark.execution.statement.StatementModel.STATEMENT_DOC_TYPE;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.exceptions.DatasourceDisabledException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.leasemanager.ConcurrencyLimitExceededException;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class AsyncQueryExecutorServiceImplSpecTest extends AsyncQueryExecutorServiceSpec {

  @Disabled("batch query is unsupported")
  public void withoutSessionCreateAsyncQueryThenGetResultThenCancel() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // disable session
    enableSession(false);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
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

  @Disabled("batch query is unsupported")
  public void sessionLimitNotImpactBatchQuery() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // disable session
    enableSession(false);
    setSessionLimit(0);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    emrsClient.startJobRunCalled(1);

    CreateAsyncQueryResponse resp2 =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    emrsClient.startJobRunCalled(2);
  }

  @Disabled("batch query is unsupported")
  public void createAsyncQueryCreateJobWithCorrectParameters() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    enableSession(false);
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
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
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getSessionId());
    Optional<StatementModel> statementModel =
        getStatement(stateStore, MYS3_DATASOURCE).apply(response.getQueryId());
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());

    // 2. reuse session id
    CreateAsyncQueryResponse second =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", MYS3_DATASOURCE, LangType.SQL, first.getSessionId()));

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
        getStatement(stateStore, MYS3_DATASOURCE).apply(first.getQueryId());
    assertTrue(firstModel.isPresent());
    assertEquals(StatementState.WAITING, firstModel.get().getStatementState());
    assertEquals(first.getQueryId(), firstModel.get().getStatementId().getId());
    assertEquals(first.getQueryId(), firstModel.get().getQueryId());
    Optional<StatementModel> secondModel =
        getStatement(stateStore, MYS3_DATASOURCE).apply(second.getQueryId());
    assertEquals(StatementState.WAITING, secondModel.get().getStatementState());
    assertEquals(second.getQueryId(), secondModel.get().getStatementId().getId());
    assertEquals(second.getQueryId(), secondModel.get().getQueryId());
  }

  @Disabled("batch query is unsupported")
  public void batchQueryHasTimeout() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    enableSession(false);
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));

    assertEquals(120L, (long) emrsClient.getJobRequest().executionTimeout());
  }

  @Test
  public void interactiveQueryNoTimeout() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    asyncQueryExecutorService.createAsyncQuery(
        new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
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
        new DataSourceMetadata.Builder()
            .setName("mybasicauth")
            .setConnector(DataSourceType.S3GLUE)
            .setProperties(properties)
            .build());
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("myselect 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getSessionId());
    Optional<StatementModel> statementModel =
        getStatement(stateStore, MYS3_DATASOURCE).apply(response.getQueryId());
    assertTrue(statementModel.isPresent());
    assertEquals(StatementState.WAITING, statementModel.get().getStatementState());

    // 2. fetch async query result. not result write to DEFAULT_RESULT_INDEX yet.
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
    updateStatementState(stateStore, MYS3_DATASOURCE).apply(mocked, StatementState.FAILED);

    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertEquals(StatementState.FAILED.getState(), asyncQueryResults.getStatus());
    assertEquals("mock error", asyncQueryResults.getError());
  }

  // https://github.com/opensearch-project/sql/issues/2344
  @Test
  public void createSessionMoreThanLimitFailed() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);
    // only allow one session in domain.
    setSessionLimit(1);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());
    setSessionState(first.getSessionId(), SessionState.RUNNING);

    // 2. create async query without session.
    ConcurrencyLimitExceededException exception =
        assertThrows(
            ConcurrencyLimitExceededException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null)));
    assertEquals("domain concurrent active session can not exceed 1", exception.getMessage());
  }

  // https://github.com/opensearch-project/sql/issues/2360
  @Test
  public void recreateSessionIfNotReady() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());

    // set sessionState to FAIL
    setSessionState(first.getSessionId(), SessionState.FAIL);

    // 2. reuse session id
    CreateAsyncQueryResponse second =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", MYS3_DATASOURCE, LangType.SQL, first.getSessionId()));

    assertNotEquals(first.getSessionId(), second.getSessionId());

    // set sessionState to FAIL
    setSessionState(second.getSessionId(), SessionState.DEAD);

    // 3. reuse session id
    CreateAsyncQueryResponse third =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", MYS3_DATASOURCE, LangType.SQL, second.getSessionId()));
    assertNotEquals(second.getSessionId(), third.getSessionId());
  }

  @Test
  public void submitQueryWithDifferentDataSourceSessionWillCreateNewSession() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "SHOW SCHEMAS IN " + MYS3_DATASOURCE, MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());

    // set sessionState to RUNNING
    setSessionState(first.getSessionId(), SessionState.RUNNING);

    // 2. reuse session id
    CreateAsyncQueryResponse second =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "SHOW SCHEMAS IN " + MYS3_DATASOURCE,
                MYS3_DATASOURCE,
                LangType.SQL,
                first.getSessionId()));

    assertEquals(first.getSessionId(), second.getSessionId());

    // set sessionState to RUNNING
    setSessionState(second.getSessionId(), SessionState.RUNNING);

    // 3. given different source, create a new session id
    CreateAsyncQueryResponse third =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "SHOW SCHEMAS IN " + MYGLUE_DATASOURCE,
                MYGLUE_DATASOURCE,
                LangType.SQL,
                second.getSessionId()));
    assertNotEquals(second.getSessionId(), third.getSessionId());
  }

  @Test
  public void recreateSessionIfStale() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());

    // set sessionState to RUNNING
    setSessionState(first.getSessionId(), SessionState.RUNNING);

    // 2. reuse session id
    CreateAsyncQueryResponse second =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", MYS3_DATASOURCE, LangType.SQL, first.getSessionId()));

    assertEquals(first.getSessionId(), second.getSessionId());

    try {
      // set timeout setting to 0
      ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
      org.opensearch.common.settings.Settings settings =
          org.opensearch.common.settings.Settings.builder()
              .put(Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS.getKeyValue(), 0)
              .build();
      request.transientSettings(settings);
      client().admin().cluster().updateSettings(request).actionGet(60000);

      // 3. not reuse session id
      CreateAsyncQueryResponse third =
          asyncQueryExecutorService.createAsyncQuery(
              new CreateAsyncQueryRequest(
                  "select 1", MYS3_DATASOURCE, LangType.SQL, second.getSessionId()));
      assertNotEquals(second.getSessionId(), third.getSessionId());
    } finally {
      // set timeout setting to 0
      ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
      org.opensearch.common.settings.Settings settings =
          org.opensearch.common.settings.Settings.builder()
              .putNull(Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS.getKeyValue())
              .build();
      request.transientSettings(settings);
      client().admin().cluster().updateSettings(request).actionGet(60000);
    }
  }

  @Test
  public void submitQueryInInvalidSessionWillCreateNewSession() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // enable session
    enableSession(true);

    // 1. create async query with invalid sessionId
    SessionId invalidSessionId = SessionId.newSessionId(MYS3_DATASOURCE);
    CreateAsyncQueryResponse asyncQuery =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "select 1", MYS3_DATASOURCE, LangType.SQL, invalidSessionId.getSessionId()));
    assertNotNull(asyncQuery.getSessionId());
    assertNotEquals(invalidSessionId.getSessionId(), asyncQuery.getSessionId());
  }

  @Test
  public void datasourceNameIncludeUppercase() {
    dataSourceService.createDataSource(
        new DataSourceMetadata.Builder()
            .setName("TESTS3")
            .setConnector(DataSourceType.S3GLUE)
            .setProperties(
                ImmutableMap.of(
                    "glue.auth.type",
                    "iam_role",
                    "glue.auth.role_arn",
                    "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                    "glue.indexstore.opensearch.uri",
                    "http://localhost:9200",
                    "glue.indexstore.opensearch.auth",
                    "noauth"))
            .build());

    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

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

  @Test
  public void concurrentSessionLimitIsDomainLevel() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // only allow one session in domain.
    setSessionLimit(1);

    // 1. create async query.
    CreateAsyncQueryResponse first =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
    assertNotNull(first.getSessionId());
    setSessionState(first.getSessionId(), SessionState.RUNNING);

    // 2. create async query without session.
    ConcurrencyLimitExceededException exception =
        assertThrows(
            ConcurrencyLimitExceededException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest(
                        "select 1", MYGLUE_DATASOURCE, LangType.SQL, null)));
    assertEquals("domain concurrent active session can not exceed 1", exception.getMessage());
  }

  @Test
  public void testDatasourceDisabled() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // Disable Datasource
    HashMap<String, Object> datasourceMap = new HashMap<>();
    datasourceMap.put("name", MYS3_DATASOURCE);
    datasourceMap.put("status", DataSourceStatus.DISABLED);
    this.dataSourceService.patchDataSource(datasourceMap);

    // 1. create async query.
    try {
      asyncQueryExecutorService.createAsyncQuery(
          new CreateAsyncQueryRequest("select 1", MYS3_DATASOURCE, LangType.SQL, null));
      fail("It should have thrown DataSourceDisabledException");
    } catch (DatasourceDisabledException exception) {
      Assertions.assertEquals("Datasource mys3 is disabled.", exception.getMessage());
    }
  }
}
