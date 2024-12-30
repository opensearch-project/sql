/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.xcontent.StatementModelXContentSerializer;

@RequiredArgsConstructor
public class OpenSearchStatementStorageService implements StatementStorageService {
  private static final Logger LOG = LogManager.getLogger();

  private final StateStore stateStore;
  private final StatementModelXContentSerializer serializer;

  @Override
  public StatementModel createStatement(
      StatementModel statementModel, AsyncQueryRequestContext asyncQueryRequestContext) {
    try {
      return stateStore.create(
          statementModel.getId(),
          statementModel,
          StatementModel::copy,
          OpenSearchStateStoreUtil.getIndexName(statementModel.getDatasourceName()));
    } catch (VersionConflictEngineException e) {
      String errorMsg = "statement already exist. " + statementModel.getStatementId();
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  @Override
  public Optional<StatementModel> getStatement(
      String id, String datasourceName, AsyncQueryRequestContext asyncQueryRequestContext) {
    return stateStore.get(
        id, serializer::fromXContent, OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  @Override
  public StatementModel updateStatementState(
      StatementModel oldStatementModel,
      StatementState statementState,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    try {
      return stateStore.updateState(
          oldStatementModel,
          statementState,
          StatementModel::copyWithState,
          OpenSearchStateStoreUtil.getIndexName(oldStatementModel.getDatasourceName()));
    } catch (DocumentMissingException e) {
      String errorMsg =
          String.format(
              "cancel statement failed. no statement found. statement: %s.",
              oldStatementModel.getStatementId());
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    } catch (VersionConflictEngineException e) {
      StatementModel statementModel =
          getStatement(
                  oldStatementModel.getId(),
                  oldStatementModel.getDatasourceName(),
                  asyncQueryRequestContext)
              .orElse(oldStatementModel);
      String errorMsg =
          String.format(
              "cancel statement failed. current statementState: %s " + "statement: %s.",
              statementModel.getStatementState(), statementModel.getStatementId());
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }
}
