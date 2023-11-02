/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;
import static org.opensearch.sql.spark.execution.session.SessionId.newSessionId;
import static org.opensearch.sql.spark.execution.session.SessionModel.initSession;
import static org.opensearch.sql.spark.execution.statement.StatementId.newStatementId;
import static org.opensearch.sql.spark.execution.statement.StatementModel.submitStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatementModelByQueryId;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionType;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Process async query request. */
@RequiredArgsConstructor
public abstract class AsyncQueryHandler {
  private final JobExecutionResponseReader jobExecutionResponseReader;
  private final StateStore stateStore;

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    Statement statement = getStatementByQueryId(asyncQueryJobMetadata.getQueryId());
    StatementState statementState = statement.getStatementState();
    if (statementState == StatementState.SUCCESS || statementState == StatementState.FAILED) {
      String queryId = asyncQueryJobMetadata.getQueryId().getId();
      JSONObject result =
          jobExecutionResponseReader.getResultWithQueryId(
              queryId, asyncQueryJobMetadata.getResultIndex());
      if (result.has(DATA_FIELD)) {
        JSONObject items = result.getJSONObject(DATA_FIELD);

        // If items have STATUS_FIELD, use it; otherwise, mark failed
        String status = items.optString(STATUS_FIELD, JobRunState.FAILED.toString());
        result.put(STATUS_FIELD, status);

        // If items have ERROR_FIELD, use it; otherwise, set empty string
        String error = items.optString(ERROR_FIELD, "");
        result.put(ERROR_FIELD, error);
        return result;
      }
    }
    JSONObject result = new JSONObject();
    result.put(STATUS_FIELD, statementState.getState());
    result.put(ERROR_FIELD, Optional.of(statement.getStatementModel().getError()).orElse(""));
    return result;
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    getStatementByQueryId(asyncQueryJobMetadata.getQueryId()).cancel();
    return asyncQueryJobMetadata.getQueryId().getId();
  }

  public abstract DispatchQueryResponse submit(
      DispatchQueryRequest request, DispatchQueryContext context);

  protected Statement getStatementByQueryId(AsyncQueryId queryId) {
    return new Statement(stateStore, getStatementModelByQueryId(stateStore, queryId));
  }

  // todo, refactor code after extract StartJobRequest logic in configuration file.
  public void createSessionAndStatement(
      DispatchQueryRequest dispatchQueryRequest,
      String appId,
      String jobId,
      SessionType sessionType,
      String datasourceName,
      AsyncQueryId queryId) {
    String qid = queryId.getId();
    SessionId sessionId = newSessionId(datasourceName);
    StateStore.createSession(stateStore, datasourceName)
        .apply(initSession(appId, jobId, sessionId, sessionType, datasourceName));
    StateStore.createStatement(stateStore, datasourceName)
        .apply(
            submitStatement(
                sessionId,
                appId,
                jobId,
                newStatementId(qid),
                dispatchQueryRequest.getLangType(),
                datasourceName,
                dispatchQueryRequest.getQuery(),
                queryId.getId()));
  }
}
