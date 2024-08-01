/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Optional;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

/**
 * Abstraction over flint index state storage. Flint index state will maintain the status of each
 * flint index.
 */
public interface FlintIndexStateModelService {

  /**
   * Create Flint index state record
   *
   * @param flintIndexStateModel the model to be saved
   * @param asyncQueryRequestContext the request context passed to AsyncQueryExecutorService
   * @return saved model
   */
  FlintIndexStateModel createFlintIndexStateModel(
      FlintIndexStateModel flintIndexStateModel, AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Get Flint index state record
   *
   * @param id ID(latestId) of the Flint index state record
   * @param datasourceName datasource name
   * @param asyncQueryRequestContext the request context passed to AsyncQueryExecutorService
   * @return retrieved model
   */
  Optional<FlintIndexStateModel> getFlintIndexStateModel(
      String id, String datasourceName, AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Update Flint index state record
   *
   * @param flintIndexStateModel the model to be updated
   * @param flintIndexState new state
   * @param datasourceName Datasource name
   * @param asyncQueryRequestContext the request context passed to AsyncQueryExecutorService
   * @return Updated model
   */
  FlintIndexStateModel updateFlintIndexState(
      FlintIndexStateModel flintIndexStateModel,
      FlintIndexState flintIndexState,
      String datasourceName,
      AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Delete Flint index state record
   *
   * @param id ID(latestId) of the Flint index state record
   * @param datasourceName datasource name
   * @param asyncQueryRequestContext the request context passed to AsyncQueryExecutorService
   * @return true if deleted, otherwise false
   */
  boolean deleteFlintIndexStateModel(
      String id, String datasourceName, AsyncQueryRequestContext asyncQueryRequestContext);
}
