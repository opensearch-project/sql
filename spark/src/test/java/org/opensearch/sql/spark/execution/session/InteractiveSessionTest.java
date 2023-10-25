/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.InteractiveSessionTest.TestSession.testSession;
import static org.opensearch.sql.spark.execution.session.SessionManagerTest.sessionSetting;
import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;
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
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.test.OpenSearchIntegTestCase;

/** mock-maker-inline does not work with OpenSearchTestCase. */
public class InteractiveSessionTest extends OpenSearchIntegTestCase {

  private static final String DS_NAME = "mys3";
  private static final String indexName = DATASOURCE_TO_REQUEST_INDEX.apply(DS_NAME);

  private TestEMRServerlessClient emrsClient;
  private StartJobRequest startJobRequest;
  private StateStore stateStore;

  @Before
  public void setup() {
    emrsClient = new TestEMRServerlessClient();
    startJobRequest = new StartJobRequest("", "", "appId", "", "", new HashMap<>(), false, "");
    stateStore = new StateStore(client(), clusterService());
  }

  @After
  public void clean() {
    if (clusterService().state().routingTable().hasIndex(indexName)) {
      client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }
  }

  @Test
  public void openCloseSession() {
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(SessionId.newSessionId(DS_NAME))
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();

    // open session
    TestSession testSession = testSession(session, stateStore);
    testSession
        .open(createSessionRequest())
        .assertSessionState(NOT_STARTED)
        .assertAppId("appId")
        .assertJobId("jobId");
    emrsClient.startJobRunCalled(1);

    // close session
    testSession.close();
    emrsClient.cancelJobRunCalled(1);
  }

  @Test
  public void openSessionFailedConflict() {
    SessionId sessionId = SessionId.newSessionId(DS_NAME);
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();
    session.open(createSessionRequest());

    InteractiveSession duplicateSession =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .stateStore(stateStore)
            .serverlessClient(emrsClient)
            .build();
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> duplicateSession.open(createSessionRequest()));
    assertEquals("session already exist. " + sessionId, exception.getMessage());
  }

  @Test
  public void closeNotExistSession() {
    SessionId sessionId = SessionId.newSessionId(DS_NAME);
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .stateStore(stateStore)
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
    Session session =
        new SessionManager(stateStore, emrsClient, sessionSetting(false))
            .createSession(createSessionRequest());

    TestSession testSession = testSession(session, stateStore);
    testSession.assertSessionState(NOT_STARTED).assertAppId("appId").assertJobId("jobId");
  }

  @Test
  public void sessionManagerGetSession() {
    SessionManager sessionManager =
        new SessionManager(stateStore, emrsClient, sessionSetting(false));
    Session session = sessionManager.createSession(createSessionRequest());

    Optional<Session> managerSession = sessionManager.getSession(session.getSessionId());
    assertTrue(managerSession.isPresent());
    assertEquals(session.getSessionId(), managerSession.get().getSessionId());
  }

  @Test
  public void sessionManagerGetSessionNotExist() {
    SessionManager sessionManager =
        new SessionManager(stateStore, emrsClient, sessionSetting(false));

    Optional<Session> managerSession =
        sessionManager.getSession(SessionId.newSessionId("no-exist"));
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
          getSession(stateStore, DS_NAME).apply(session.getSessionModel().getId());
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

  public static CreateSessionRequest createSessionRequest() {
    return new CreateSessionRequest(
        "jobName",
        "appId",
        "arn",
        SparkSubmitParameters.Builder.builder(),
        new HashMap<>(),
        "resultIndex",
        DS_NAME);
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
