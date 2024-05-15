/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;

@RequiredArgsConstructor
public class OpenSearchStatementStorageService implements StatementStorageService {

  private final StateStore stateStore;

  @Override
  public StatementModel createStatement(StatementModel statementModel, String datasourceName) {
    return stateStore.create(
        statementModel, StatementModel::copy, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public Optional<StatementModel> getStatement(String id, String datasourceName) {
    return stateStore.get(
        id, StatementModel::fromXContent, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public StatementModel updateStatementState(
      StatementModel oldStatementModel, StatementState statementState, String datasourceName) {
    return stateStore.updateState(
        oldStatementModel,
        statementState,
        StatementModel::copyWithState,
        DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }
}
