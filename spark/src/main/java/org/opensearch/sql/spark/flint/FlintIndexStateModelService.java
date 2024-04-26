/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Optional;

/**
 * Abstraction over flint index state storage.
 * Flint index state will maintain the status of each flint index.
 */
public interface FlintIndexStateModelService {
  FlintIndexStateModel createFlintIndexStateModel(
      FlintIndexStateModel flintIndexStateModel, String datasourceName);

  Optional<FlintIndexStateModel> getFlintIndexStateModel(String id, String datasourceName);

  FlintIndexStateModel updateFlintIndexState(
      FlintIndexStateModel flintIndexStateModel,
      FlintIndexState flintIndexState,
      String datasourceName);

  boolean deleteFlintIndexStateModel(String id, String datasourceName);
}
