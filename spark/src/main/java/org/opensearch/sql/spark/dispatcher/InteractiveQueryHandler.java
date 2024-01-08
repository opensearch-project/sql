/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SESSION_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.JOB_TYPE_TAG_KEY;

import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.utils.MetricUtils;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.CreateSessionRequest;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statement.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@RequiredArgsConstructor
public class InteractiveQueryHandler extends AsyncQueryHandler {
  private final SessionManager sessionManager;
  private final JobExecutionResponseReader jobExecutionResponseReader;
  private final LeaseManager leaseManager;

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

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    Session session = null;
    String clusterName = dispatchQueryRequest.getClusterName();
    String jobName = clusterName + ":" + "non-index-query";
    Map<String, String> tags = context.getTags();
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();

    // todo, manage lease lifecycle
    leaseManager.borrow(
        new LeaseRequest(JobType.INTERACTIVE, dispatchQueryRequest.getDatasource()));

    if (dispatchQueryRequest.getSessionId() != null) {
      // get session from request
      SessionId sessionId = new SessionId(dispatchQueryRequest.getSessionId());
      Optional<Session> createdSession = sessionManager.getSession(sessionId);
      if (createdSession.isPresent()) {
        session = createdSession.get();
      }
    }
    if (session == null
        || !session.isOperationalForDataSource(dispatchQueryRequest.getDatasource())) {
      // create session if not exist or session dead/fail
      tags.put(JOB_TYPE_TAG_KEY, JobType.INTERACTIVE.getText());
      session =
          sessionManager.createSession(
              new CreateSessionRequest(
                  jobName,
                  dispatchQueryRequest.getApplicationId(),
                  dispatchQueryRequest.getExecutionRoleARN(),
                  SparkSubmitParameters.Builder.builder()
                      .className(FLINT_SESSION_CLASS_NAME)
                      .clusterName(clusterName)
                      .dataSource(dataSourceMetadata)
                      .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams()),
                  tags,
                  dataSourceMetadata.getResultIndex(),
                  dataSourceMetadata.getName()));
      MetricUtils.incrementNumericalMetric(MetricName.EMR_INTERACTIVE_QUERY_JOBS_CREATION_COUNT);
    }
    session.submit(
        new QueryRequest(
            context.getQueryId(),
            dispatchQueryRequest.getLangType(),
            dispatchQueryRequest.getQuery()));
    return new DispatchQueryResponse(
        context.getQueryId(),
        session.getSessionModel().getJobId(),
        dataSourceMetadata.getResultIndex(),
        session.getSessionId().getSessionId());
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
