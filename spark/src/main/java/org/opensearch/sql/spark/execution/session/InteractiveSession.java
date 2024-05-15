/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionModel.initInteractiveSession;
import static org.opensearch.sql.spark.execution.session.SessionState.DEAD;
import static org.opensearch.sql.spark.execution.session.SessionState.END_STATE;
import static org.opensearch.sql.spark.execution.session.SessionState.FAIL;
import static org.opensearch.sql.spark.execution.statement.StatementId.newStatementId;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.execution.statement.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.TimeProvider;

/**
 * Interactive session.
 *
 * <p>ENTRY_STATE: not_started
 */
@Getter
@Builder
public class InteractiveSession implements Session {
  private static final Logger LOG = LogManager.getLogger();

  public static final String SESSION_ID_TAG_KEY = "sid";

  private final SessionId sessionId;
  private final SessionStorageService sessionStorageService;
  private final StatementStorageService statementStorageService;
  private final EMRServerlessClient serverlessClient;
  private SessionModel sessionModel;
  // the threshold of elapsed time in milliseconds before we say a session is stale
  private long sessionInactivityTimeoutMilli;
  private TimeProvider timeProvider;

  @Override
  public void open(CreateSessionRequest createSessionRequest) {
    try {
      // append session id;
      createSessionRequest
          .getSparkSubmitParameters()
          .acceptModifier(
              (parameters) -> {
                parameters.sessionExecution(
                    sessionId.getSessionId(), createSessionRequest.getDatasourceName());
              });
      createSessionRequest.getTags().put(SESSION_ID_TAG_KEY, sessionId.getSessionId());
      StartJobRequest startJobRequest =
          createSessionRequest.getStartJobRequest(sessionId.getSessionId());
      String jobID = serverlessClient.startJobRun(startJobRequest);
      String applicationId = startJobRequest.getApplicationId();

      sessionModel =
          initInteractiveSession(
              applicationId, jobID, sessionId, createSessionRequest.getDatasourceName());
      sessionStorageService.createSession(sessionModel);
    } catch (VersionConflictEngineException e) {
      String errorMsg = "session already exist. " + sessionId;
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  /** todo. StatementSweeper will delete doc. */
  @Override
  public void close() {
    Optional<SessionModel> model =
        sessionStorageService.getSession(sessionModel.getId(), sessionModel.getDatasourceName());
    if (model.isEmpty()) {
      throw new IllegalStateException("session does not exist. " + sessionModel.getSessionId());
    } else {
      serverlessClient.cancelJobRun(
          sessionModel.getApplicationId(), sessionModel.getJobId(), false);
    }
  }

  /** Submit statement. If submit successfully, Statement in waiting state. */
  public StatementId submit(QueryRequest request) {
    Optional<SessionModel> model =
        sessionStorageService.getSession(sessionModel.getId(), sessionModel.getDatasourceName());
    if (model.isEmpty()) {
      throw new IllegalStateException("session does not exist. " + sessionModel.getSessionId());
    } else {
      sessionModel = model.get();
      if (!END_STATE.contains(sessionModel.getSessionState())) {
        String qid = request.getQueryId().getId();
        StatementId statementId = newStatementId(qid);
        Statement st =
            Statement.builder()
                .sessionId(sessionId)
                .applicationId(sessionModel.getApplicationId())
                .jobId(sessionModel.getJobId())
                .statementStorageService(statementStorageService)
                .statementId(statementId)
                .langType(LangType.SQL)
                .datasourceName(sessionModel.getDatasourceName())
                .query(request.getQuery())
                .queryId(qid)
                .build();
        st.open();
        return statementId;
      } else {
        String errMsg =
            String.format(
                "can't submit statement, session should not be in end state, "
                    + "current session state is: %s",
                sessionModel.getSessionState().getSessionState());
        LOG.debug(errMsg);
        throw new IllegalStateException(errMsg);
      }
    }
  }

  @Override
  public Optional<Statement> get(StatementId stID) {
    return statementStorageService
        .getStatement(stID.getId(), sessionModel.getDatasourceName())
        .map(
            model ->
                Statement.builder()
                    .sessionId(sessionId)
                    .applicationId(model.getApplicationId())
                    .jobId(model.getJobId())
                    .statementId(model.getStatementId())
                    .langType(model.getLangType())
                    .query(model.getQuery())
                    .queryId(model.getQueryId())
                    .statementStorageService(statementStorageService)
                    .statementModel(model)
                    .build());
  }

  @Override
  public boolean isOperationalForDataSource(String dataSourceName) {
    boolean isSessionStateValid =
        sessionModel.getSessionState() != DEAD && sessionModel.getSessionState() != FAIL;
    boolean isDataSourceMatch = sessionId.getDataSourceName().equals(dataSourceName);
    boolean isSessionUpdatedRecently =
        timeProvider.currentEpochMillis() - sessionModel.getLastUpdateTime()
            <= sessionInactivityTimeoutMilli;

    return isSessionStateValid && isDataSourceMatch && isSessionUpdatedRecently;
  }
}
