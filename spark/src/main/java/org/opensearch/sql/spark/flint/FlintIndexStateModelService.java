package org.opensearch.sql.spark.flint;

import java.util.Optional;

public interface FlintIndexStateModelService {
  FlintIndexStateModel updateFlintIndexState(
      FlintIndexStateModel flintIndexStateModel,
      FlintIndexState flintIndexState,
      String datasourceName);

  Optional<FlintIndexStateModel> getFlintIndexStateModel(String id, String datasourceName);

  FlintIndexStateModel createFlintIndexStateModel(
      FlintIndexStateModel flintIndexStateModel, String datasourceName);

  boolean deleteFlintIndexStateModel(String id, String datasourceName);
}
