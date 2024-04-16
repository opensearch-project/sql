/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;

/**
 * Interface for accessing {@link StatementModel} data storage. {@link StatementModel} is an
 * abstraction over the query request within a Session.
 */
public interface StatementStorageService {

  StatementModel createStatement(StatementModel statementModel);

  StatementModel updateStatementState(
      StatementModel oldStatementModel, StatementState statementState);

  Optional<StatementModel> getStatement(String id, String datasourceName);
}
