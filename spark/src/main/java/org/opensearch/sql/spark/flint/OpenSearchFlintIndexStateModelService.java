package org.opensearch.sql.spark.flint;

import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.execution.statestore.StateStore;

@RequiredArgsConstructor
public class OpenSearchFlintIndexStateModelService implements FlintIndexStateModelService {

  private final StateStore stateStore;

  @Override
  public FlintIndexStateModel updateFlintIndexState(
      FlintIndexStateModel flintIndexStateModel,
      FlintIndexState flintIndexState,
      String datasourceName) {
    return stateStore.updateState(
        flintIndexStateModel,
        flintIndexState,
        FlintIndexStateModel::copyWithState,
        DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public Optional<FlintIndexStateModel> getFlintIndexStateModel(String id, String datasourceName) {
    return stateStore.get(
        id, FlintIndexStateModel::fromXContent, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public FlintIndexStateModel createFlintIndexStateModel(
      FlintIndexStateModel flintIndexStateModel, String datasourceName) {
    return stateStore.create(
        flintIndexStateModel,
        FlintIndexStateModel::copy,
        DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public boolean deleteFlintIndexStateModel(String id, String datasourceName) {
    return stateStore.delete(id, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }
}
