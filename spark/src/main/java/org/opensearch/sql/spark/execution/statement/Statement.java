/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.statement.StatementModel.submitStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.createStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.rest.model.LangType;

/** Statement represent query to execute in session. One statement map to one session. */
@Getter
@Builder
public class Statement {
  private static final Logger LOG = LogManager.getLogger();

  private final SessionId sessionId;
  private final String applicationId;
  private final String jobId;
  private final StatementId statementId;
  private final LangType langType;
  private final String datasourceName;
  private final String query;
  private final String queryId;
  private final StateStore stateStore;

  @Setter private StatementModel statementModel;

  public Statement(StateStore stateStore, StatementModel model) {
    this(
        model.getSessionId(),
        model.getApplicationId(),
        model.getJobId(),
        model.getStatementId(),
        model.getLangType(),
        model.getDatasourceName(),
        model.getQuery(),
        model.getQueryId(),
        stateStore,
        model);
  }

  public Statement(
      SessionId sessionId,
      String applicationId,
      String jobId,
      StatementId statementId,
      LangType langType,
      String datasourceName,
      String query,
      String queryId,
      StateStore stateStore) {
    this(
        sessionId,
        applicationId,
        jobId,
        statementId,
        langType,
        datasourceName,
        query,
        queryId,
        stateStore,
        null);
  }

  public Statement(
      SessionId sessionId,
      String applicationId,
      String jobId,
      StatementId statementId,
      LangType langType,
      String datasourceName,
      String query,
      String queryId,
      StateStore stateStore,
      StatementModel statementModel) {
    this.sessionId = sessionId;
    this.applicationId = applicationId;
    this.jobId = jobId;
    this.statementId = statementId;
    this.langType = langType;
    this.datasourceName = datasourceName;
    this.query = query;
    this.queryId = queryId;
    this.stateStore = stateStore;
    this.statementModel = statementModel;
  }

  /** Open a statement. */
  public void open() {
    try {
      statementModel =
          submitStatement(
              sessionId,
              applicationId,
              jobId,
              statementId,
              langType,
              datasourceName,
              query,
              queryId);
      statementModel = createStatement(stateStore, datasourceName).apply(statementModel);
    } catch (VersionConflictEngineException e) {
      String errorMsg = "statement already exist. " + statementId;
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  /** Cancel a statement. */
  public void cancel() {
    StatementState statementState = statementModel.getStatementState();

    if (statementState.equals(StatementState.SUCCESS)
        || statementState.equals(StatementState.FAILED)
        || statementState.equals(StatementState.CANCELLED)) {
      String errorMsg =
          String.format(
              "can't cancel statement in %s state. statement: %s.",
              statementState.getState(), statementId);
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
    try {
      this.statementModel =
          updateStatementState(stateStore, statementModel.getDatasourceName())
              .apply(this.statementModel, StatementState.CANCELLED);
    } catch (DocumentMissingException e) {
      String errorMsg =
          String.format("cancel statement failed. no statement found. statement: %s.", statementId);
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    } catch (VersionConflictEngineException e) {
      this.statementModel =
          getStatement(stateStore, statementModel.getDatasourceName())
              .apply(statementModel.getId())
              .orElse(this.statementModel);
      String errorMsg =
          String.format(
              "cancel statement failed. current statementState: %s " + "statement: %s.",
              this.statementModel.getStatementState(), statementId);
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  public StatementState getStatementState() {
    return statementModel.getStatementState();
  }
}
