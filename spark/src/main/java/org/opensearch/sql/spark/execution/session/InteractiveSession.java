/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionModel.initInteractiveSession;
import static org.opensearch.sql.spark.execution.session.SessionState.END_STATE;
import static org.opensearch.sql.spark.execution.statement.StatementId.newStatementId;
import static org.opensearch.sql.spark.execution.statestore.StateStore.createSession;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getSession;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statement.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.rest.model.LangType;

/**
 * Interactive session.
 *
 * <p>ENTRY_STATE: not_started
 */
@Getter
@Builder
public class InteractiveSession implements Session {
  private static final Logger LOG = LogManager.getLogger();

  private final SessionId sessionId;
  private final StateStore stateStore;
  private final EMRServerlessClient serverlessClient;
  private SessionModel sessionModel;

  @Override
  public void open(CreateSessionRequest createSessionRequest) {
    try {
      // append session id;
      createSessionRequest
          .getSparkSubmitParametersBuilder()
          .sessionExecution(sessionId.getSessionId());
      String jobID = serverlessClient.startJobRun(createSessionRequest.getStartJobRequest());
      String applicationId = createSessionRequest.getStartJobRequest().getApplicationId();

      sessionModel =
          initInteractiveSession(
              applicationId, jobID, sessionId, createSessionRequest.getDatasourceName());
      createSession(stateStore, sessionModel.getDatasourceName()).apply(sessionModel);
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
        getSession(stateStore, sessionModel.getDatasourceName()).apply(sessionModel.getId());
    if (model.isEmpty()) {
      throw new IllegalStateException("session does not exist. " + sessionModel.getSessionId());
    } else {
      serverlessClient.cancelJobRun(sessionModel.getApplicationId(), sessionModel.getJobId());
    }
  }

  /** Submit statement. If submit successfully, Statement in waiting state. */
  public StatementId submit(QueryRequest request) {
    Optional<SessionModel> model =
        getSession(stateStore, sessionModel.getDatasourceName()).apply(sessionModel.getId());
    if (model.isEmpty()) {
      throw new IllegalStateException("session does not exist. " + sessionModel.getSessionId());
    } else {
      sessionModel = model.get();
      if (!END_STATE.contains(sessionModel.getSessionState())) {
        StatementId statementId = newStatementId();
        Statement st =
            Statement.builder()
                .sessionId(sessionId)
                .applicationId(sessionModel.getApplicationId())
                .jobId(sessionModel.getJobId())
                .stateStore(stateStore)
                .statementId(statementId)
                .langType(LangType.SQL)
                .datasourceName(sessionModel.getDatasourceName())
                .query(request.getQuery())
                .queryId(statementId.getId())
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
    return StateStore.getStatement(stateStore, sessionModel.getDatasourceName())
        .apply(stID.getId())
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
                    .stateStore(stateStore)
                    .statementModel(model)
                    .build());
  }
}
