/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.session.SessionManagerTest.sessionSetting;
import static org.opensearch.sql.spark.execution.statement.StatementState.CANCELLED;
import static org.opensearch.sql.spark.execution.statement.StatementState.WAITING;
import static org.opensearch.sql.spark.execution.statement.StatementTest.TestStatement.testStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateSessionState;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import java.util.HashMap;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.data.constants.SparkConstants;
import org.opensearch.sql.spark.execution.session.CreateSessionRequest;
import org.opensearch.sql.spark.execution.session.InteractiveSessionTest;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.test.OpenSearchIntegTestCase;

public class StatementTest extends OpenSearchIntegTestCase {

  private static final String indexName = SparkConstants.SPARK_REQUEST_BUFFER_INDEX_NAME;

  private StartJobRequest startJobRequest;
  private StateStore stateStore;
  private InteractiveSessionTest.TestEMRServerlessClient emrsClient =
      new InteractiveSessionTest.TestEMRServerlessClient();

  @Before
  public void setup() {
    startJobRequest = new StartJobRequest("", "", "appId", "", "", new HashMap<>(), false, "");
    stateStore = new StateStore(indexName, client(), clusterService());
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
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .build();

    // submit statement
    TestStatement testStatement = testStatement(st, stateStore);
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
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
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
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
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
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
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
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .build();
    st.open();

    StatementModel running =
        updateStatementState(stateStore).apply(st.getStatementModel(), CANCELLED);

    assertEquals(StatementState.CANCELLED, running.getStatementState());

    // cancel conflict
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format(
            "cancel statement failed. current statementState: CANCELLED " + "statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void cancelRunningStatementFailed() {
    StatementId stId = new StatementId("statementId");
    Statement st =
        Statement.builder()
            .sessionId(new SessionId("sessionId"))
            .applicationId("appId")
            .jobId("jobId")
            .statementId(stId)
            .langType(LangType.SQL)
            .query("query")
            .queryId("statementId")
            .stateStore(stateStore)
            .build();
    st.open();

    // update to running state
    StatementModel model = st.getStatementModel();
    st.setStatementModel(
        StatementModel.copyWithState(
            st.getStatementModel(),
            StatementState.RUNNING,
            model.getSeqNo(),
            model.getPrimaryTerm()));

    // cancel conflict
    IllegalStateException exception = assertThrows(IllegalStateException.class, st::cancel);
    assertEquals(
        String.format("can't cancel statement in waiting state. statement: %s.", stId),
        exception.getMessage());
  }

  @Test
  public void submitStatementInRunningSession() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    // App change state to running
    updateSessionState(stateStore).apply(session.getSessionModel(), SessionState.RUNNING);

    StatementId statementId = session.submit(new QueryRequest(LangType.SQL, "select 1"));
    assertFalse(statementId.getId().isEmpty());
  }

  @Test
  public void submitStatementInNotStartedState() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    StatementId statementId = session.submit(new QueryRequest(LangType.SQL, "select 1"));
    assertFalse(statementId.getId().isEmpty());
  }

  @Test
  public void failToSubmitStatementInDeadState() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    updateSessionState(stateStore).apply(session.getSessionModel(), SessionState.DEAD);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> session.submit(new QueryRequest(LangType.SQL, "select 1")));
    assertEquals(
        "can't submit statement, session should not be in end state, current session state is:"
            + " dead",
        exception.getMessage());
  }

  @Test
  public void failToSubmitStatementInFailState() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    updateSessionState(stateStore).apply(session.getSessionModel(), SessionState.FAIL);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> session.submit(new QueryRequest(LangType.SQL, "select 1")));
    assertEquals(
        "can't submit statement, session should not be in end state, current session state is:"
            + " fail",
        exception.getMessage());
  }

  @Test
  public void newStatementFieldAssert() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));
    StatementId statementId = session.submit(new QueryRequest(LangType.SQL, "select 1"));
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
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    // other's delete session
    client()
        .delete(new DeleteRequest(indexName, session.getSessionId().getSessionId()))
        .actionGet();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> session.submit(new QueryRequest(LangType.SQL, "select 1")));
    assertEquals("session does not exist. " + session.getSessionId(), exception.getMessage());
  }

  @Test
  public void getStatementSuccess() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));
    // App change state to running
    updateSessionState(stateStore).apply(session.getSessionModel(), SessionState.RUNNING);
    StatementId statementId = session.submit(new QueryRequest(LangType.SQL, "select 1"));

    Optional<Statement> statement = session.get(statementId);
    assertTrue(statement.isPresent());
    assertEquals(WAITING, statement.get().getStatementState());
    assertEquals(statementId, statement.get().getStatementId());
  }

  @Test
  public void getStatementNotExist() {
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));
    // App change state to running
    updateSessionState(stateStore).apply(session.getSessionModel(), SessionState.RUNNING);

    Optional<Statement> statement = session.get(StatementId.newStatementId());
    assertFalse(statement.isPresent());
  }

  @RequiredArgsConstructor
  static class TestStatement {
    private final Statement st;
    private final StateStore stateStore;

    public static TestStatement testStatement(Statement st, StateStore stateStore) {
      return new TestStatement(st, stateStore);
    }

    public TestStatement assertSessionState(StatementState expected) {
      assertEquals(expected, st.getStatementModel().getStatementState());

      Optional<StatementModel> model = getStatement(stateStore).apply(st.getStatementId().getId());
      assertTrue(model.isPresent());
      assertEquals(expected, model.get().getStatementState());

      return this;
    }

    public TestStatement assertStatementId(StatementId expected) {
      assertEquals(expected, st.getStatementModel().getStatementId());

      Optional<StatementModel> model = getStatement(stateStore).apply(st.getStatementId().getId());
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
  }
}
