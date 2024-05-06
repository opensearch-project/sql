/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

/** Operation to drop Flint index */
public class FlintIndexOpDrop extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  public FlintIndexOpDrop(
      FlintIndexStateModelService flintIndexStateModelService,
      String datasourceName,
      EMRServerlessClientFactory emrServerlessClientFactory) {
    super(flintIndexStateModelService, datasourceName, emrServerlessClientFactory);
  }

  public boolean validate(FlintIndexState state) {
    return state == FlintIndexState.REFRESHING
        || state == FlintIndexState.EMPTY
        || state == FlintIndexState.ACTIVE
        || state == FlintIndexState.CREATING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.DELETING;
  }

  /** cancel EMR-S job, wait cancelled state upto 15s. */
  @SneakyThrows
  @Override
  void runOp(FlintIndexMetadata flintIndexMetadata, FlintIndexStateModel flintIndexStateModel) {
    LOG.debug(
        "Performing drop index operation for index: {}",
        flintIndexMetadata.getOpensearchIndexName());
    cancelStreamingJob(flintIndexStateModel);
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.DELETED;
  }
}
