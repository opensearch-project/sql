/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/** Cancel refreshing job. */
public class FlintIndexOpCancel extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final EMRServerlessClient emrServerlessClient;

  public FlintIndexOpCancel(
      StateStore stateStore, String datasourceName, EMRServerlessClient emrServerlessClient) {
    super(stateStore, datasourceName);
    this.emrServerlessClient = emrServerlessClient;
  }

  public boolean validate(FlintIndexState state) {
    return state == FlintIndexState.REFRESHING || state == FlintIndexState.CANCELLING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.CANCELLING;
  }

  /** cancel EMR-S job, wait cancelled state upto 15s. */
  @SneakyThrows
  @Override
  void runOp(FlintIndexStateModel flintIndexStateModel) {
    String applicationId = flintIndexStateModel.getApplicationId();
    String jobId = flintIndexStateModel.getJobId();
    try {
      emrServerlessClient.cancelJobRun(
          flintIndexStateModel.getApplicationId(), flintIndexStateModel.getJobId());
    } catch (IllegalArgumentException e) {
      // handle job does not exist case.
      LOG.error(e);
      return;
    }

    // pull job state until timeout or cancelled.
    String jobRunState = "";
    int count = 3;
    while (count-- != 0) {
      jobRunState =
          emrServerlessClient.getJobRunResult(applicationId, jobId).getJobRun().getState();
      if (jobRunState.equalsIgnoreCase("Cancelled")) {
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    if (!jobRunState.equalsIgnoreCase("Cancelled")) {
      String errMsg = "cancel job timeout";
      LOG.error(errMsg);
      throw new TimeoutException(errMsg);
    }
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.ACTIVE;
  }
}
