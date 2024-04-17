package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.xcontent.StatementModelXContentSerializer;

@RequiredArgsConstructor
public class OpenSearchStatementStorageService implements StatementStorageService {

  private final StateStore stateStore;

  @Override
  public StatementModel createStatement(StatementModel statementModel, String datasourceName) {
    return stateStore.create(
        statementModel, StatementModel::copy, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
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

  @Override
  public Optional<StatementModel> getStatementModel(String id, String datasourceName) {
    StatementModelXContentSerializer serializer = new StatementModelXContentSerializer();
    return stateStore.get(
        id, serializer::fromXContent, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }
}
