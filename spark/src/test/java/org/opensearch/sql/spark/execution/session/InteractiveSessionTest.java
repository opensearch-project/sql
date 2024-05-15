/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.session.SessionManagerTest.sessionSetting;
import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.session.SessionTestUtil.createSessionRequest;

import java.util.HashMap;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statestore.OpenSearchSessionStorageService;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStatementStorageService;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;
import org.opensearch.test.OpenSearchIntegTestCase;

/** mock-maker-inline does not work with OpenSearchTestCase. */
public class InteractiveSessionTest extends OpenSearchIntegTestCase {

  private static final String indexName =
      OpenSearchStateStoreUtil.getIndexName(TEST_DATASOURCE_NAME);

  private TestEMRServerlessClient emrsClient;
  private StartJobRequest startJobRequest;
  private SessionStorageService sessionStorageService;
  private StatementStorageService statementStorageService;
  private SessionManager sessionManager;

  @Before
  public void setup() {
    emrsClient = new TestEMRServerlessClient();
    startJobRequest = new StartJobRequest("", "appId", "", "", new HashMap<>(), false, "");
    StateStore stateStore = new StateStore(client(), clusterService());
    sessionStorageService = new OpenSearchSessionStorageService(stateStore);
    statementStorageService = new OpenSearchStatementStorageService(stateStore);
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    sessionManager =
        new SessionManager(
            sessionStorageService,
            statementStorageService,
            emrServerlessClientFactory,
            sessionSetting());
  }

  @After
  public void clean() {
    if (clusterService().state().routingTable().hasIndex(indexName)) {
      client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }
  }

  @Test
  public void openCloseSession() {
    SessionId sessionId = SessionId.newSessionId(TEST_DATASOURCE_NAME);
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .statementStorageService(statementStorageService)
            .sessionStorageService(sessionStorageService)
            .serverlessClient(emrsClient)
            .build();

    SessionAssertions assertions = new SessionAssertions(session);
    assertions
        .open(createSessionRequest())
        .assertSessionState(NOT_STARTED)
        .assertAppId("appId")
        .assertJobId("jobId");
    emrsClient.startJobRunCalled(1);
    emrsClient.assertJobNameOfLastRequest(
        TEST_CLUSTER_NAME + ":" + JobType.INTERACTIVE.getText() + ":" + sessionId.getSessionId());

    // close session
    assertions.close();
    emrsClient.cancelJobRunCalled(1);
  }

  @Test
  public void openSessionFailedConflict() {
    SessionId sessionId = SessionId.newSessionId(TEST_DATASOURCE_NAME);
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .sessionStorageService(sessionStorageService)
            .statementStorageService(statementStorageService)
            .serverlessClient(emrsClient)
            .build();
    session.open(createSessionRequest());

    InteractiveSession duplicateSession =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .sessionStorageService(sessionStorageService)
            .statementStorageService(statementStorageService)
            .serverlessClient(emrsClient)
            .build();
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> duplicateSession.open(createSessionRequest()));
    assertEquals("session already exist. " + sessionId, exception.getMessage());
  }

  @Test
  public void closeNotExistSession() {
    SessionId sessionId = SessionId.newSessionId(TEST_DATASOURCE_NAME);
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .sessionStorageService(sessionStorageService)
            .statementStorageService(statementStorageService)
            .serverlessClient(emrsClient)
            .build();
    session.open(createSessionRequest());

    client().delete(new DeleteRequest(indexName, sessionId.getSessionId())).actionGet();

    IllegalStateException exception = assertThrows(IllegalStateException.class, session::close);
    assertEquals("session does not exist. " + sessionId, exception.getMessage());
    emrsClient.cancelJobRunCalled(0);
  }

  @Test
  public void sessionManagerCreateSession() {
    Session session = sessionManager.createSession(createSessionRequest());

    new SessionAssertions(session)
        .assertSessionState(NOT_STARTED)
        .assertAppId("appId")
        .assertJobId("jobId");
  }

  @Test
  public void sessionManagerGetSession() {
    Session session = sessionManager.createSession(createSessionRequest());

    Optional<Session> managerSession = sessionManager.getSession(session.getSessionId());
    assertTrue(managerSession.isPresent());
    assertEquals(session.getSessionId(), managerSession.get().getSessionId());
  }

  @Test
  public void sessionManagerGetSessionNotExist() {
    Optional<Session> managerSession =
        sessionManager.getSession(SessionId.newSessionId("no-exist"));
    assertTrue(managerSession.isEmpty());
  }

  @RequiredArgsConstructor
  class SessionAssertions {
    private final Session session;

    public SessionAssertions assertSessionState(SessionState expected) {
      assertEquals(expected, session.getSessionModel().getSessionState());

      Optional<SessionModel> sessionStoreState =
          sessionStorageService.getSession(session.getSessionModel().getId(), TEST_DATASOURCE_NAME);
      assertTrue(sessionStoreState.isPresent());
      assertEquals(expected, sessionStoreState.get().getSessionState());

      return this;
    }

    public SessionAssertions assertAppId(String expected) {
      assertEquals(expected, session.getSessionModel().getApplicationId());
      return this;
    }

    public SessionAssertions assertJobId(String expected) {
      assertEquals(expected, session.getSessionModel().getJobId());
      return this;
    }

    public SessionAssertions open(CreateSessionRequest req) {
      session.open(req);
      return this;
    }

    public SessionAssertions close() {
      session.close();
      return this;
    }
  }
}
