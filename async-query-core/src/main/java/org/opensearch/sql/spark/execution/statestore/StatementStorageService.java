/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;

/**
 * Interface for accessing {@link StatementModel} data storage. {@link StatementModel} is an
 * abstraction over the query request within a Session.
 */
public interface StatementStorageService {

  StatementModel createStatement(
      StatementModel statementModel, AsyncQueryRequestContext asyncQueryRequestContext);

  StatementModel updateStatementState(
      StatementModel oldStatementModel,
      StatementState statementState,
      AsyncQueryRequestContext asyncQueryRequestContext);

  Optional<StatementModel> getStatement(
      String id, String datasourceName, AsyncQueryRequestContext asyncQueryRequestContext);
}
