/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.constants.TestConstants.TEST_DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.session.SessionTestUtil.createSessionRequest;
import static org.opensearch.sql.spark.execution.statement.StatementState.CANCELLED;
import static org.opensearch.sql.spark.execution.statement.StatementState.RUNNING;
import static org.opensearch.sql.spark.execution.statement.StatementState.WAITING;
import static org.opensearch.sql.spark.execution.statement.StatementTest.TestStatement.testStatement;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.session.DatasourceEmbeddedSessionIdProvider;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionConfigSupplier;
import org.opensearch.sql.spark.execution.session.SessionIdProvider;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.session.TestEMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.OpenSearchSessionStorageService;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStatementStorageService;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.StatementModelXContentSerializer;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.IDUtils;
import org.opensearch.test.OpenSearchIntegTestCase;

public class StatementTest extends OpenSearchIntegTestCase {
  private static final String indexName =
      OpenSearchStateStoreUtil.getIndexName(TEST_DATASOURCE_NAME);

  private StatementStorageService statementStorageService;
  private SessionStorageService sessionStorageService;
  private final TestEMRServerlessClient emrsClient = new TestEMRServerlessClient();
  private final SessionConfigSupplier sessionConfigSupplier = () -> 600000L;
  private final SessionIdProvider sessionIdProvider = new DatasourceEmbeddedSessionIdProvider();

  private SessionManager sessionManager;
  private final AsyncQueryRequestContext asyncQueryRequestContext =
      new NullAsyncQueryRequestContext();

  @Before
  public void setup() {
    StateStore stateStore = new StateStore(client(), clusterService());
    statementStorageService =
        new OpenSearchStatementStorageService(stateStore, new StatementModelXContentSerializer());
    sessionStorageService =
        new OpenSearchSessionStorageService(stateStore, new SessionModelXContentSerializer());
    EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;

    sessionManager =
        new SessionManager(
            sessionStorageService,
            statementStorageService,
            emrServerlessClientFactory,
            sessionConfigSupplier,
            sessionIdProvider);
  }

  @After
  public void clean() {
    if (clusterService().state().routingTable().hasIndex(indexName)) {
      client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }
  }

  @Test
  public void openThenCancelStatement() {
    Statement st = buildStatement();

    // submit statement
    TestStatement testStatement = testStatement(st, statementStorageService);
    testStatement
        .open()
        .assertSessionState(WAITING)
        .assertStatementId(new StatementId("statementId"));

    // close statement
    testStatement.cancel().assertSessionState(CANCELLED);
  }

  private Statement buildStatement() {
    return buildStatement(new StatementId("statementId"));
  }

  private Statement buildStatement(StatementId stId) {
    return Statement.builder()
        .sessionId("sessionId")
        .applicationId("appId")
        .jobId("jobId")
        .statementId(stId)
        .langType(LangType.SQL)
        .datasourceName(TEST_DATASOURCE_NAME)
        .query("query")
        .queryId("statementId")
        .statementStorageService(statementStorageService)
        .build();
  }

  @Test
  public void openFailedBecauseConflict() {
    Statement st = buildStatement();
    st.open();

    // open statement with same statement id
    Statement dupSt = buildStatement();
    IllegalStateException exception = assertThrows(IllegalStateException.class, dupSt::open);
    assertEquals("statement already exist. statementId=statementId", exception.getMessage());
  }

  @Test
  public void cancelNotExistStatement_throwsException() {
    StatementId stId = new StatementId("statementId");
    Statement st = buildStatement(stId);
    st.open();

    client().delete(new DeleteRequest(indexName, stId.getId())).actionGet();

    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format("cancel statement failed. no statement found. statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelFailedBecauseOfConflict() {
    StatementId stId = new StatementId("statementId");
    Statement st = buildStatement(stId);
    st.open();

    StatementModel running =
        statementStorageService.updateStatementState(
            st.getStatementModel(), CANCELLED, asyncQueryRequestContext);

    assertEquals(StatementState.CANCELLED, running.getStatementState());
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format(
            "cancel statement failed. current statementState: CANCELLED " + "statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelCancelledStatement_throwsException() {
    testCancelThrowsExceptionGivenStatementState(StatementState.CANCELLED);
  }

  @Test
  public void cancelSuccessStatement_throwsException() {
    testCancelThrowsExceptionGivenStatementState(StatementState.SUCCESS);
  }

  @Test
  public void cancelFailedStatement_throwsException() {
    testCancelThrowsExceptionGivenStatementState(StatementState.FAILED);
  }

  @Test
  public void cancelTimeoutStatement_throwsException() {
    testCancelThrowsExceptionGivenStatementState(StatementState.TIMEOUT);
  }

  private void testCancelThrowsExceptionGivenStatementState(StatementState state) {
    StatementId stId = new StatementId("statementId");
    Statement st = createStatement(stId);

    StatementModel model = st.getStatementModel();
    st.setStatementModel(
        StatementModel.copyWithState(st.getStatementModel(), state, model.getMetadata()));

    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format("can't cancel statement in %s state. statement: %s.", state.getState(), stId),
        exception.getMessage());
  }

  @Test
  public void cancelRunningStatementSuccess() {
    Statement st = buildStatement();

    // submit statement
    TestStatement testStatement = testStatement(st, statementStorageService);
    testStatement
        .open()
        .assertSessionState(WAITING)
        .assertStatementId(new StatementId("statementId"));

    testStatement.run();

    // close statement
    testStatement.cancel().assertSessionState(CANCELLED);
  }

  @Test
  public void submitStatementInRunningSession() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);

    // App change state to running
    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.RUNNING);

    StatementId statementId = session.submit(queryRequest(), asyncQueryRequestContext);
    assertFalse(statementId.getId().isEmpty());
  }

  @Test
  public void submitStatementInNotStartedState() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);

    StatementId statementId = session.submit(queryRequest(), asyncQueryRequestContext);
    assertFalse(statementId.getId().isEmpty());
  }

  @Test
  public void failToSubmitStatementInDeadState() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);

    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.DEAD);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> session.submit(queryRequest(), asyncQueryRequestContext));
    assertEquals(
        "can't submit statement, session should not be in end state, current session state is:"
            + " dead",
        exception.getMessage());
  }

  @Test
  public void failToSubmitStatementInFailState() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);

    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.FAIL);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> session.submit(queryRequest(), asyncQueryRequestContext));
    assertEquals(
        "can't submit statement, session should not be in end state, current session state is:"
            + " fail",
        exception.getMessage());
  }

  @Test
  public void newStatementFieldAssert() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);
    StatementId statementId = session.submit(queryRequest(), asyncQueryRequestContext);
    Optional<Statement> statement = session.get(statementId, asyncQueryRequestContext);

    assertTrue(statement.isPresent());
    assertEquals(session.getSessionId(), statement.get().getSessionId());
    assertEquals("appId", statement.get().getApplicationId());
    assertEquals("jobId", statement.get().getJobId());
    assertEquals(statementId, statement.get().getStatementId());
    assertEquals(WAITING, statement.get().getStatementState());
    assertEquals(LangType.SQL, statement.get().getLangType());
    assertEquals("select 1", statement.get().getQuery());
  }

  @Test
  public void failToSubmitStatementInDeletedSession() {
    EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);

    // other's delete session
    client().delete(new DeleteRequest(indexName, session.getSessionId())).actionGet();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> session.submit(queryRequest(), asyncQueryRequestContext));
    assertEquals("session does not exist. " + session.getSessionId(), exception.getMessage());
  }

  @Test
  public void getStatementSuccess() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);
    // App change state to running
    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.RUNNING);
    StatementId statementId = session.submit(queryRequest(), asyncQueryRequestContext);

    Optional<Statement> statement = session.get(statementId, asyncQueryRequestContext);
    assertTrue(statement.isPresent());
    assertEquals(WAITING, statement.get().getStatementState());
    assertEquals(statementId, statement.get().getStatementId());
  }

  @Test
  public void getStatementNotExist() {
    Session session =
        sessionManager.createSession(createSessionRequest(), asyncQueryRequestContext);
    // App change state to running
    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.RUNNING);

    Optional<Statement> statement =
        session.get(StatementId.newStatementId("not-exist-id"), asyncQueryRequestContext);
    assertFalse(statement.isPresent());
  }

  @RequiredArgsConstructor
  static class TestStatement {
    private final Statement st;
    private final StatementStorageService statementStorageService;

    public static TestStatement testStatement(
        Statement st, StatementStorageService statementStorageService) {
      return new TestStatement(st, statementStorageService);
    }

    public TestStatement assertSessionState(StatementState expected) {
      assertEquals(expected, st.getStatementModel().getStatementState());

      Optional<StatementModel> model =
          statementStorageService.getStatement(
              st.getStatementId().getId(), TEST_DATASOURCE_NAME, st.getAsyncQueryRequestContext());
      assertTrue(model.isPresent());
      assertEquals(expected, model.get().getStatementState());

      return this;
    }

    public TestStatement assertStatementId(StatementId expected) {
      assertEquals(expected, st.getStatementModel().getStatementId());

      Optional<StatementModel> model =
          statementStorageService.getStatement(
              st.getStatementId().getId(), TEST_DATASOURCE_NAME, st.getAsyncQueryRequestContext());
      assertTrue(model.isPresent());
      assertEquals(expected, model.get().getStatementId());
      return this;
    }

    public TestStatement open() {
      st.open();
      return this;
    }

    public TestStatement cancel() {
      st.cancel();
      return this;
    }

    public TestStatement run() {
      StatementModel model =
          statementStorageService.updateStatementState(
              st.getStatementModel(), RUNNING, st.getAsyncQueryRequestContext());
      st.setStatementModel(model);
      return this;
    }
  }

  private QueryRequest queryRequest() {
    return new QueryRequest(IDUtils.encode(TEST_DATASOURCE_NAME), LangType.SQL, "select 1");
  }

  private Statement createStatement(StatementId stId) {
    Statement st = buildStatement(stId);
    st.open();
    return st;
  }
}
