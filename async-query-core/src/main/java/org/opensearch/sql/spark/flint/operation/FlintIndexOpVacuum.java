/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexClient;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.scheduler.AsyncQueryScheduler;

/** Flint index vacuum operation. */
public class FlintIndexOpVacuum extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final AsyncQueryScheduler asyncQueryScheduler;

  /** OpenSearch client. */
  private final FlintIndexClient flintIndexClient;

  public FlintIndexOpVacuum(
      FlintIndexStateModelService flintIndexStateModelService,
      String datasourceName,
      FlintIndexClient flintIndexClient,
      EMRServerlessClientFactory emrServerlessClientFactory,
      AsyncQueryScheduler asyncQueryScheduler) {
    super(flintIndexStateModelService, datasourceName, emrServerlessClientFactory);
    this.flintIndexClient = flintIndexClient;
    this.asyncQueryScheduler = asyncQueryScheduler;
  }

  @Override
  boolean validate(FlintIndexState state) {
    return state == FlintIndexState.DELETED;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.VACUUMING;
  }

  @Override
  public void runOp(
      FlintIndexMetadata flintIndexMetadata,
      FlintIndexStateModel flintIndex,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    LOG.info("Vacuuming Flint index {}", flintIndexMetadata.getOpensearchIndexName());
    if (flintIndexMetadata.getFlintIndexOptions().isExternalScheduler()) {
      asyncQueryScheduler.removeJob(flintIndexMetadata.getOpensearchIndexName());
    }
    flintIndexClient.deleteIndex(flintIndexMetadata.getOpensearchIndexName());
  }

  @Override
  FlintIndexState stableState() {
    // Instruct StateStore to purge the index state doc
    return FlintIndexState.NONE;
  }
}
