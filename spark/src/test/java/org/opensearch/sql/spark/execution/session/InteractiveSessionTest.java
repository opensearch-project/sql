/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.InteractiveSessionTest.TestSession.testSession;
import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getSession;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import java.util.HashMap;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/** mock-maker-inline does not work with OpenSearchTestCase. */
public class InteractiveSessionTest extends OpenSearchSingleNodeTestCase {

  private static final String indexName = "mockindex";

  private TestEMRServerlessClient emrsClient;
  private StartJobRequest startJobRequest;
  private StateStore stateStore;

  @Before
  public void setup() {
    emrsClient = new TestEMRServerlessClient();
    startJobRequest = new StartJobRequest("", "", "appId", "", "", new HashMap<>(), false, "");
    stateStore = new StateStore(indexName, client());
    createIndex(indexName);
  }

  @After
  public void clean() {
    client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
  }

  @Test
  public void openCloseSession() {
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(SessionId.newSessionId())
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();

    // open session
    TestSession testSession = testSession(session, stateStore);
    testSession.open(new CreateSessionRequest(startJobRequest, "datasource")).assertSessionState(NOT_STARTED).assertAppId("appId").assertJobId("jobId");
    emrsClient.startJobRunCalled(1);

    // close session
    testSession.close();
    emrsClient.cancelJobRunCalled(1);
  }

  @Test
  public void openSessionFailedConflict() {
    SessionId sessionId = new SessionId("duplicate-session-id");
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();
    session.open(new CreateSessionRequest(startJobRequest, "datasource"));

    InteractiveSession duplicateSession =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> duplicateSession.open(new CreateSessionRequest(startJobRequest, "datasource")));
    assertEquals("session already exist. sessionId=duplicate-session-id", exception.getMessage());
  }

  @Test
  public void closeNotExistSession() {
    SessionId sessionId = SessionId.newSessionId();
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();
    session.open(new CreateSessionRequest(startJobRequest, "datasource"));

    client().delete(new DeleteRequest(indexName, sessionId.getSessionId())).actionGet();

    IllegalStateException exception = assertThrows(IllegalStateException.class, session::close);
    assertEquals("session does not exist. " + sessionId, exception.getMessage());
    emrsClient.cancelJobRunCalled(0);
  }

  @Test
  public void sessionManagerCreateSession() {
    Session session =
        new SessionManager(stateStore, emrsClient)
            .createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    TestSession testSession = testSession(session, stateStore);
    testSession.assertSessionState(NOT_STARTED).assertAppId("appId").assertJobId("jobId");
  }

  @Test
  public void sessionManagerGetSession() {
    SessionManager sessionManager = new SessionManager(stateStore, emrsClient);
    Session session =
        sessionManager.createSession(new CreateSessionRequest(startJobRequest, "datasource"));

    Optional<Session> managerSession = sessionManager.getSession(session.getSessionId());
    assertTrue(managerSession.isPresent());
    assertEquals(session.getSessionId(), managerSession.get().getSessionId());
  }

  @Test
  public void sessionManagerGetSessionNotExist() {
    SessionManager sessionManager = new SessionManager(stateStore, emrsClient);

    Optional<Session> managerSession = sessionManager.getSession(new SessionId("no-exist"));
    assertTrue(managerSession.isEmpty());
  }

  @RequiredArgsConstructor
  static class TestSession {
    private final Session session;
    private final StateStore stateStore;

    public static TestSession testSession(Session session, StateStore stateStore) {
      return new TestSession(session, stateStore);
    }

    public TestSession assertSessionState(SessionState expected) {
      assertEquals(expected, session.getSessionModel().getSessionState());

      Optional<SessionModel> sessionStoreState =
          getSession(stateStore).apply(session.getSessionModel().getId());
      assertTrue(sessionStoreState.isPresent());
      assertEquals(expected, sessionStoreState.get().getSessionState());

      return this;
    }

    public TestSession assertAppId(String expected) {
      assertEquals(expected, session.getSessionModel().getApplicationId());
      return this;
    }

    public TestSession assertJobId(String expected) {
      assertEquals(expected, session.getSessionModel().getJobId());
      return this;
    }

    public TestSession open(CreateSessionRequest req) {
      session.open(req);
      return this;
    }

    public TestSession close() {
      session.close();
      return this;
    }
  }

  public static class TestEMRServerlessClient implements EMRServerlessClient {

    private int startJobRunCalled = 0;
    private int cancelJobRunCalled = 0;

    @Override
    public String startJobRun(StartJobRequest startJobRequest) {
      startJobRunCalled++;
      return "jobId";
    }

    @Override
    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
      return null;
    }

    @Override
    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
      cancelJobRunCalled++;
      return null;
    }

    public void startJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, startJobRunCalled);
    }

    public void cancelJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, cancelJobRunCalled);
    }
  }
}
