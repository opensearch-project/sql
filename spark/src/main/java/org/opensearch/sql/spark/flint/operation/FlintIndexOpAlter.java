/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/**
 * Index Operation for Altering the flint index. Only handles alter operation when
 * auto_refresh=false.
 */
public class FlintIndexOpAlter extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger(FlintIndexOpAlter.class);
  private final EMRServerlessClient emrServerlessClient;
  private final FlintIndexMetadataService flintIndexMetadataService;
  private final FlintIndexOptions flintIndexOptions;

  public FlintIndexOpAlter(
      FlintIndexOptions flintIndexOptions,
      StateStore stateStore,
      String datasourceName,
      EMRServerlessClient emrServerlessClient,
      FlintIndexMetadataService flintIndexMetadataService) {
    super(stateStore, datasourceName);
    this.emrServerlessClient = emrServerlessClient;
    this.flintIndexMetadataService = flintIndexMetadataService;
    this.flintIndexOptions = flintIndexOptions;
  }

  @Override
  protected boolean validate(FlintIndexState state) {
    return state == FlintIndexState.ACTIVE || state == FlintIndexState.REFRESHING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.UPDATING;
  }

  @SneakyThrows
  @Override
  void runOp(FlintIndexMetadata flintIndexMetadata, FlintIndexStateModel flintIndexStateModel) {
    LOG.debug(
        "Running alter index operation for index: {}", flintIndexMetadata.getOpensearchIndexName());
    this.flintIndexMetadataService.updateIndexToManualRefresh(
        flintIndexMetadata.getOpensearchIndexName(), flintIndexOptions);
    cancelStreamingJob(emrServerlessClient, flintIndexStateModel);
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.ACTIVE;
  }
}
