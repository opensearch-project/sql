package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;

public interface StatementStorageService {

  StatementModel createStatement(StatementModel statementModel, String datasourceName);

  StatementModel updateStatementState(
      StatementModel oldStatementModel, StatementState statementState, String datasourceName);

  Optional<StatementModel> getStatement(String id, String datasourceName);
}
