/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.scheduler.AsyncQueryScheduler;
import org.opensearch.sql.spark.scheduler.model.AsyncQuerySchedulerRequest;

/** Operation to drop Flint index */
public class FlintIndexOpDrop extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final AsyncQueryScheduler asyncQueryScheduler;

  public FlintIndexOpDrop(
      FlintIndexStateModelService flintIndexStateModelService,
      String datasourceName,
      EMRServerlessClientFactory emrServerlessClientFactory,
      AsyncQueryScheduler asyncQueryScheduler) {
    super(flintIndexStateModelService, datasourceName, emrServerlessClientFactory);
    this.asyncQueryScheduler = asyncQueryScheduler;
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
  void runOp(
      FlintIndexMetadata flintIndexMetadata,
      FlintIndexStateModel flintIndexStateModel,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    LOG.debug(
        "Performing drop index operation for index: {}",
        flintIndexMetadata.getOpensearchIndexName());
    if (flintIndexMetadata.getFlintIndexOptions().isExternalScheduler()) {
      AsyncQuerySchedulerRequest request = new AsyncQuerySchedulerRequest();
      request.setAccountId(flintIndexStateModel.getAccountId());
      request.setDataSource(flintIndexStateModel.getDatasourceName());
      request.setJobId(flintIndexMetadata.getOpensearchIndexName());
      asyncQueryScheduler.unscheduleJob(request);
    } else {
      cancelStreamingJob(flintIndexStateModel);
    }
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.DELETED;
  }
}
