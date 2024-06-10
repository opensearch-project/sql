/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.xcontent.StatementModelXContentSerializer;

@RequiredArgsConstructor
public class OpenSearchStatementStorageService implements StatementStorageService {

  private final StateStore stateStore;
  private final StatementModelXContentSerializer serializer;

  @Override
  public StatementModel createStatement(
      StatementModel statementModel, AsyncQueryRequestContext asyncQueryRequestContext) {
    return stateStore.create(
        statementModel.getId(),
        statementModel,
        StatementModel::copy,
        OpenSearchStateStoreUtil.getIndexName(statementModel.getDatasourceName()));
  }

  @Override
  public Optional<StatementModel> getStatement(String id, String datasourceName) {
    return stateStore.get(
        id, serializer::fromXContent, OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  @Override
  public StatementModel updateStatementState(
      StatementModel oldStatementModel, StatementState statementState) {
    return stateStore.updateState(
        oldStatementModel,
        statementState,
        StatementModel::copyWithState,
        OpenSearchStateStoreUtil.getIndexName(oldStatementModel.getDatasourceName()));
  }
}
