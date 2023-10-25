/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@RequiredArgsConstructor
public class InteractiveQueryHandler extends AsyncQueryHandler {
  private final SessionManager sessionManager;
  private final JobExecutionResponseReader jobExecutionResponseReader;

  @Override
  protected JSONObject getResponseFromResultIndex(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    String queryId = asyncQueryJobMetadata.getQueryId().getId();
    return jobExecutionResponseReader.getResultWithQueryId(
        queryId, asyncQueryJobMetadata.getResultIndex());
  }

  @Override
  protected JSONObject getResponseFromExecutor(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    JSONObject result = new JSONObject();
    String queryId = asyncQueryJobMetadata.getQueryId().getId();
    Statement statement = getStatementByQueryId(asyncQueryJobMetadata.getSessionId(), queryId);
    StatementState statementState = statement.getStatementState();
    result.put(STATUS_FIELD, statementState.getState());
    result.put(ERROR_FIELD, Optional.of(statement.getStatementModel().getError()).orElse(""));
    return result;
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    String queryId = asyncQueryJobMetadata.getQueryId().getId();
    getStatementByQueryId(asyncQueryJobMetadata.getSessionId(), queryId).cancel();
    return queryId;
  }

  private Statement getStatementByQueryId(String sid, String qid) {
    SessionId sessionId = new SessionId(sid);
    Optional<Session> session = sessionManager.getSession(sessionId);
    if (session.isPresent()) {
      // todo, statementId == jobId if statement running in session.
      StatementId statementId = new StatementId(qid);
      Optional<Statement> statement = session.get().get(statementId);
      if (statement.isPresent()) {
        return statement.get();
      } else {
        throw new IllegalArgumentException("no statement found. " + statementId);
      }
    } else {
      throw new IllegalArgumentException("no session found. " + sessionId);
    }
  }
}
