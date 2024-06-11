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

/** Cancel refreshing job for refresh query when user clicks cancel button on UI. */
public class FlintIndexOpCancel extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  public FlintIndexOpCancel(
      FlintIndexStateModelService flintIndexStateModelService,
      String datasourceName,
      EMRServerlessClientFactory emrServerlessClientFactory) {
    super(flintIndexStateModelService, datasourceName, emrServerlessClientFactory);
  }

  // Only in refreshing state, the job is cancellable in case of REFRESH query.
  public boolean validate(FlintIndexState state) {
    return state == FlintIndexState.REFRESHING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.CANCELLING;
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
    return FlintIndexState.ACTIVE;
  }
}
