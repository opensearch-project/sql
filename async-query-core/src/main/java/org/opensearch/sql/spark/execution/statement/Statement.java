/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.statement.StatementModel.submitStatement;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;
import org.opensearch.sql.spark.rest.model.LangType;

/** Statement represent query to execute in session. One statement map to one session. */
@Getter
@Builder
public class Statement {
  private static final Logger LOG = LogManager.getLogger();

  private final String sessionId;
  // optional
  private final String accountId;
  private final String applicationId;
  private final String jobId;
  private final StatementId statementId;
  private final LangType langType;
  private final String datasourceName;
  private final String query;
  private final String queryId;
  private final AsyncQueryRequestContext asyncQueryRequestContext;
  private final StatementStorageService statementStorageService;

  @Setter private StatementModel statementModel;

  /** Open a statement. */
  public void open() {
    statementModel =
        submitStatement(
            sessionId,
            accountId,
            applicationId,
            jobId,
            statementId,
            langType,
            datasourceName,
            query,
            queryId);
    statementModel =
        statementStorageService.createStatement(statementModel, asyncQueryRequestContext);
  }

  /** Cancel a statement. */
  public void cancel() {
    StatementState statementState = statementModel.getStatementState();

    if (statementState.equals(StatementState.SUCCESS)
        || statementState.equals(StatementState.FAILED)
        || statementState.equals(StatementState.TIMEOUT)
        || statementState.equals(StatementState.CANCELLED)) {
      String errorMsg =
          String.format(
              "can't cancel statement in %s state. statement: %s.",
              statementState.getState(), statementId);
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
    this.statementModel =
        statementStorageService.updateStatementState(statementModel, StatementState.CANCELLED);
  }

  public StatementState getStatementState() {
    return statementModel.getStatementState();
  }
}
