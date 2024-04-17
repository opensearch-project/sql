/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.session.InteractiveSessionTest.createSessionRequest;
import static org.opensearch.sql.spark.execution.session.SessionManagerTest.sessionSetting;
import static org.opensearch.sql.spark.execution.statement.StatementState.CANCELLED;
import static org.opensearch.sql.spark.execution.statement.StatementState.RUNNING;
import static org.opensearch.sql.spark.execution.statement.StatementState.WAITING;
import static org.opensearch.sql.spark.execution.statement.StatementTest.TestStatement.testStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.session.InteractiveSessionTest;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statestore.OpensearchSessionStorageService;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.test.OpenSearchIntegTestCase;

public class StatementTest extends OpenSearchIntegTestCase {

  private static final String DS_NAME = "mys3";
  private static final String indexName = DATASOURCE_TO_REQUEST_INDEX.apply(DS_NAME);

  private StateStore stateStore;
  private StatementStorageService statementStorageService;
  private SessionStorageService sessionStorageService;
  private InteractiveSessionTest.TestEMRServerlessClient emrsClient =
      new InteractiveSessionTest.TestEMRServerlessClient();

  @Before
  public void setup() {
    stateStore = new StateStore(client(), clusterService());
    statementStorageService = new OpenSearchStatementStorageService(stateStore);
    sessionStorageService = new OpensearchSessionStorageService(stateStore);
  }

  @After
  public void clean() {
    if (clusterService().state().routingTable().hasIndex(indexName)) {
      client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }
  }

  @Test
  public void openThenCancelStatement() {
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(new StatementId("statementId"))
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();

    // submit statement
    TestStatement testStatement = testStatement(st, statementStorageService);
    testStatement
        .open()
        .assertSessionState(WAITING)
        .assertStatementId(new StatementId("statementId"));

    // close statement
    testStatement.cancel().assertSessionState(CANCELLED);
  }

  @Test
  public void openFailedBecauseConflict() {
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(new StatementId("statementId"))
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();
    st.open();

    // open statement with same statement id
    Statement dupSt =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(new StatementId("statementId"))
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();
    IllegalStateException exception = assertThrows(IllegalStateException.class, dupSt::open);
    assertEquals("statement already exist. statementId=statementId", exception.getMessage());
  }

  @Test
  public void cancelNotExistStatement() {
    StatementId stId = new StatementId("statementId");
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(stId)
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();
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
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(stId)
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();
    st.open();

    StatementModel running =
        statementStorageService.updateStatementState(st.getStatementModel(), CANCELLED, DS_NAME);
    assertEquals(StatementState.CANCELLED, running.getStatementState());

    // cancel conflict
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format(
            "cancel statement failed. current statementState: CANCELLED " + "statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelSuccessStatementFailed() {
    StatementId stId = new StatementId("statementId");
    Statement st = createStatement(stId);

    // update to running state
    StatementModel model = st.getStatementModel();
    st.setStatementModel(
        StatementModel.copyWithState(
            st.getStatementModel(),
            StatementState.SUCCESS,
            model.getSeqNo(),
            model.getPrimaryTerm()));

    // cancel conflict
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format("can't cancel statement in success state. statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelFailedStatementFailed() {
    StatementId stId = new StatementId("statementId");
    Statement st = createStatement(stId);

    // update to running state
    StatementModel model = st.getStatementModel();
    st.setStatementModel(
        StatementModel.copyWithState(
            st.getStatementModel(),
            StatementState.FAILED,
            model.getSeqNo(),
            model.getPrimaryTerm()));

    // cancel conflict
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format("can't cancel statement in failed state. statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelCancelledStatementFailed() {
    StatementId stId = new StatementId("statementId");
    Statement st = createStatement(stId);

    // update to running state
    StatementModel model = st.getStatementModel();
    st.setStatementModel(
        StatementModel.copyWithState(
            st.getStatementModel(), CANCELLED, model.getSeqNo(), model.getPrimaryTerm()));

    // cancel conflict
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format("can't cancel statement in cancelled state. statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelRunningStatementSuccess() {
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(new StatementId("statementId"))
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();

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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());

    // App change state to running
    sessionStorageService.updateSessionState(
        session.getSessionModel(), SessionState.RUNNING, DS_NAME);

    StatementId statementId = session.submit(queryRequest());
    assertFalse(statementId.getId().isEmpty());
  }

  @Test
  public void submitStatementInNotStartedState() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());

    StatementId statementId = session.submit(queryRequest());
    assertFalse(statementId.getId().isEmpty());
  }

  @Test
  public void failToSubmitStatementInDeadState() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());

    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.DEAD, DS_NAME);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> session.submit(queryRequest()));
    assertEquals(
        "can't submit statement, session should not be in end state, current session state is:"
            + " dead",
        exception.getMessage());
  }

  @Test
  public void failToSubmitStatementInFailState() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());

    sessionStorageService.updateSessionState(session.getSessionModel(), SessionState.FAIL, DS_NAME);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> session.submit(queryRequest()));
    assertEquals(
        "can't submit statement, session should not be in end state, current session state is:"
            + " fail",
        exception.getMessage());
  }

  @Test
  public void newStatementFieldAssert() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());
    StatementId statementId = session.submit(queryRequest());
    Optional<Statement> statement = session.get(statementId);

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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());

    // other's delete session
    client()
        .delete(new DeleteRequest(indexName, session.getSessionId().getSessionId()))
        .actionGet();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> session.submit(queryRequest()));
    assertEquals("session does not exist. " + session.getSessionId(), exception.getMessage());
  }

  @Test
  public void getStatementSuccess() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());
    // App change state to running
    sessionStorageService.updateSessionState(
        session.getSessionModel(), SessionState.RUNNING, DS_NAME);
    StatementId statementId = session.submit(queryRequest());

    Optional<Statement> statement = session.get(statementId);
    assertTrue(statement.isPresent());
    assertEquals(WAITING, statement.get().getStatementState());
    assertEquals(statementId, statement.get().getStatementId());
  }

  @Test
  public void getStatementNotExist() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                stateStore,
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                sessionSetting())
            .createSession(createSessionRequest());
    // App change state to running
    sessionStorageService.updateSessionState(
        session.getSessionModel(), SessionState.RUNNING, DS_NAME);

    Optional<Statement> statement = session.get(StatementId.newStatementId("not-exist-id"));
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
          statementStorageService.getStatementModel(st.getStatementId().getId(), DS_NAME);
      assertTrue(model.isPresent());
      assertEquals(expected, model.get().getStatementState());

      return this;
    }

    public TestStatement assertStatementId(StatementId expected) {
      assertEquals(expected, st.getStatementModel().getStatementId());

      Optional<StatementModel> model =
          statementStorageService.getStatementModel(st.getStatementId().getId(), DS_NAME);
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
          statementStorageService.updateStatementState(st.getStatementModel(), RUNNING, DS_NAME);
      st.setStatementModel(model);
      return this;
    }
  }

  private QueryRequest queryRequest() {
    return new QueryRequest(AsyncQueryId.newAsyncQueryId(DS_NAME), LangType.SQL, "select 1");
  }

  private Statement createStatement(StatementId stId) {
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(stId)
            .langType(LangType.SQL)
            .datasourceName(DS_NAME)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .statementStorageService(statementStorageService)
            .build();
    st.open();
    return st;
  }
}
