/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.InteractiveSessionTest.TestSession.testSession;
import static org.opensearch.sql.spark.execution.session.SessionManagerTest.sessionSetting;
import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

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
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statement.OpenSearchStatementStorageService;
import org.opensearch.sql.spark.execution.statement.StatementStorageService;
import org.opensearch.sql.spark.execution.statestore.OpensearchSessionStorageService;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.test.OpenSearchIntegTestCase;

/** mock-maker-inline does not work with OpenSearchTestCase. */
public class InteractiveSessionTest extends OpenSearchIntegTestCase {

  private static final String DS_NAME = "mys3";
  private static final String indexName = DATASOURCE_TO_REQUEST_INDEX.apply(DS_NAME);
  public static final String TEST_CLUSTER_NAME = "TEST_CLUSTER";

  private TestEMRServerlessClient emrsClient;
  private StartJobRequest startJobRequest;
  private StateStore stateStore;
  private StatementStorageService statementStorageService;
  private SessionStorageService sessionStorageService;

  @Before
  public void setup() {
    emrsClient = new TestEMRServerlessClient();
    startJobRequest = new StartJobRequest("", "appId", "", "", new HashMap<>(), false, "");
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
  public void openCloseSession() {
    SessionId sessionId = SessionId.newSessionId(DS_NAME);
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .statementStorageService(statementStorageService)
            .sessionStorageService(sessionStorageService)
            .serverlessClient(emrsClient)
            .build();

    // open session
    TestSession testSession = testSession(session, sessionStorageService);
    testSession
        .open(createSessionRequest())
        .assertSessionState(NOT_STARTED)
        .assertAppId("appId")
        .assertJobId("jobId");
    emrsClient.startJobRunCalled(1);
    emrsClient.assertJobNameOfLastRequest(
        TEST_CLUSTER_NAME + ":" + JobType.INTERACTIVE.getText() + ":" + sessionId.getSessionId());

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
            .sessionStorageService(sessionStorageService)
            .statementStorageService(statementStorageService)
            .serverlessClient(emrsClient)
            .build();
    session.open(createSessionRequest());

    InteractiveSession duplicateSession =
        InteractiveSession.builder()
            .sessionId(sessionId)
            .statementStorageService(statementStorageService)
            .sessionStorageService(sessionStorageService)
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
            .statementStorageService(statementStorageService)
            .sessionStorageService(sessionStorageService)
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    Session session =
        new SessionManager(
                statementStorageService,
                sessionStorageService,
                emrServerlessClientFactory,
                () ->
                    sessionSetting()
                        .getSettingValue(Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS))
            .createSession(createSessionRequest());

    TestSession testSession = testSession(session, sessionStorageService);
    testSession.assertSessionState(NOT_STARTED).assertAppId("appId").assertJobId("jobId");
  }

  @Test
  public void sessionManagerGetSession() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    SessionManager sessionManager =
        new SessionManager(
            statementStorageService,
            sessionStorageService,
            emrServerlessClientFactory,
            () -> sessionSetting().getSettingValue(Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS));
    Session session = sessionManager.createSession(createSessionRequest());

    Optional<Session> managerSession = sessionManager.getSession(session.getSessionId());
    assertTrue(managerSession.isPresent());
    assertEquals(session.getSessionId(), managerSession.get().getSessionId());
  }

  @Test
  public void sessionManagerGetSessionNotExist() {
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    SessionManager sessionManager =
        new SessionManager(
            statementStorageService,
            sessionStorageService,
            emrServerlessClientFactory,
            () -> sessionSetting().getSettingValue(Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS));

    Optional<Session> managerSession =
        sessionManager.getSession(SessionId.newSessionId("no-exist"));
    assertTrue(managerSession.isEmpty());
  }

  @RequiredArgsConstructor
  static class TestSession {
    private final Session session;
    private final SessionStorageService sessionStorageService;

    public static TestSession testSession(
        Session session, SessionStorageService sessionStorageService) {
      return new TestSession(session, sessionStorageService);
    }

    public TestSession assertSessionState(SessionState expected) {
      assertEquals(expected, session.getSessionModel().getSessionState());

      Optional<SessionModel> sessionStoreState =
          sessionStorageService.getSession(session.getSessionModel().getId(), DS_NAME);
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
        TEST_CLUSTER_NAME,
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

    private StartJobRequest startJobRequest;

    @Override
    public String startJobRun(StartJobRequest startJobRequest) {
      this.startJobRequest = startJobRequest;
      startJobRunCalled++;
      return "jobId";
    }

    @Override
    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
      return null;
    }

    @Override
    public CancelJobRunResult cancelJobRun(
        String applicationId, String jobId, boolean allowExceptionPropagation) {
      cancelJobRunCalled++;
      return null;
    }

    public void startJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, startJobRunCalled);
    }

    public void cancelJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, cancelJobRunCalled);
    }

    public void assertJobNameOfLastRequest(String expectedJobName) {
      assertEquals(expectedJobName, startJobRequest.getJobName());
    }
  }
}
