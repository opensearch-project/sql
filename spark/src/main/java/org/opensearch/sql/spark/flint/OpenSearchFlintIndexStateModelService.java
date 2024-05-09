/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
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
        OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  @Override
  public Optional<FlintIndexStateModel> getFlintIndexStateModel(String id, String datasourceName) {
    return stateStore.get(
        id,
        FlintIndexStateModel::fromXContent,
        OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  @Override
  public FlintIndexStateModel createFlintIndexStateModel(
      FlintIndexStateModel flintIndexStateModel, String datasourceName) {
    return stateStore.create(
        flintIndexStateModel,
        FlintIndexStateModel::copy,
        OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  @Override
  public boolean deleteFlintIndexStateModel(String id, String datasourceName) {
    return stateStore.delete(id, OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }
}
