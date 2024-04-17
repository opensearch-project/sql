package org.opensearch.sql.spark.execution.statement;

import java.util.Optional;

public interface StatementStorageService {

  StatementModel createStatement(StatementModel statementModel, String datasourceName);

  StatementModel updateStatementState(
      StatementModel oldStatementModel, StatementState statementState, String datasourceName);

  Optional<StatementModel> getStatementModel(String id, String datasourceName);
}
