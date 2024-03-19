/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import static org.opensearch.sql.spark.execution.statestore.StateStore.deleteFlintIndexState;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getFlintIndexState;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateFlintIndexState;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/** Flint Index Operation. */
@RequiredArgsConstructor
public abstract class FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final StateStore stateStore;
  private final String datasourceName;

  /** Apply operation on {@link FlintIndexMetadata} */
  public void apply(FlintIndexMetadata metadata) {
    // todo, remove this logic after IndexState feature is enabled in Flint.
    Optional<String> latestId = metadata.getLatestId();
    if (latestId.isEmpty()) {
      takeActionWithoutOCC(metadata);
    } else {
      FlintIndexStateModel initialFlintIndexStateModel = getFlintIndexStateModel(latestId.get());
      // 1.validate state.
      validFlintIndexInitialState(initialFlintIndexStateModel);

      // 2.begin, move to transitioning state
      FlintIndexStateModel transitionedFlintIndexStateModel =
          moveToTransitioningState(initialFlintIndexStateModel);
      // 3.runOp
      try {
        runOp(metadata, transitionedFlintIndexStateModel);
        commit(transitionedFlintIndexStateModel);
      } catch (Throwable e) {
        LOG.error("Rolling back transient log due to transaction operation failure", e);
        try {
          updateFlintIndexState(stateStore, datasourceName)
              .apply(transitionedFlintIndexStateModel, initialFlintIndexStateModel.getIndexState());
        } catch (Exception ex) {
          LOG.error("Failed to rollback transient log", ex);
        }
        throw e;
      }
    }
  }

  @NotNull
  private FlintIndexStateModel getFlintIndexStateModel(String latestId) {
    Optional<FlintIndexStateModel> flintIndexOptional =
        getFlintIndexState(stateStore, datasourceName).apply(latestId);
    if (flintIndexOptional.isEmpty()) {
      String errorMsg = String.format(Locale.ROOT, "no state found. docId: %s", latestId);
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
    return flintIndexOptional.get();
  }

  private void takeActionWithoutOCC(FlintIndexMetadata metadata) {
    // take action without occ.
    FlintIndexStateModel fakeModel =
        new FlintIndexStateModel(
            FlintIndexState.REFRESHING,
            metadata.getAppId(),
            metadata.getJobId(),
            "",
            datasourceName,
            System.currentTimeMillis(),
            "",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    runOp(metadata, fakeModel);
  }

  private void validFlintIndexInitialState(FlintIndexStateModel flintIndex) {
    LOG.debug("Validating the state before the transaction.");
    FlintIndexState currentState = flintIndex.getIndexState();
    if (!validate(currentState)) {
      String errorMsg =
          String.format(Locale.ROOT, "validate failed. unexpected state: [%s]", currentState);
      LOG.error(errorMsg);
      throw new IllegalStateException("Transaction failed as flint index is not in a valid state.");
    }
  }

  private FlintIndexStateModel moveToTransitioningState(FlintIndexStateModel flintIndex) {
    LOG.debug("Moving to transitioning state before committing.");
    FlintIndexState transitioningState = transitioningState();
    try {
      flintIndex =
          updateFlintIndexState(stateStore, datasourceName).apply(flintIndex, transitioningState());
    } catch (Exception e) {
      String errorMsg =
          String.format(Locale.ROOT, "Moving to transition state:%s failed.", transitioningState);
      LOG.error(errorMsg, e);
      throw new IllegalStateException(errorMsg, e);
    }
    return flintIndex;
  }

  private void commit(FlintIndexStateModel flintIndex) {
    LOG.debug("Committing the transaction and moving to stable state.");
    FlintIndexState stableState = stableState();
    try {
      if (stableState == FlintIndexState.NONE) {
        LOG.info("Deleting index state with docId: " + flintIndex.getLatestId());
        deleteFlintIndexState(stateStore, datasourceName).apply(flintIndex.getLatestId());
      } else {
        updateFlintIndexState(stateStore, datasourceName).apply(flintIndex, stableState);
      }
    } catch (Exception e) {
      String errorMsg =
          String.format(Locale.ROOT, "commit failed. target stable state: [%s]", stableState);
      LOG.error(errorMsg, e);
      throw new IllegalStateException(errorMsg, e);
    }
  }

  /***
   * Common operation between AlterOff and Drop. So moved to FlintIndexOp.
   */
  public void cancelStreamingJob(
      EMRServerlessClient emrServerlessClient, FlintIndexStateModel flintIndexStateModel)
      throws InterruptedException, TimeoutException {
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
      String errMsg =
          "Cancel job timeout for Application ID: " + applicationId + ", Job ID: " + jobId;
      LOG.error(errMsg);
      throw new TimeoutException("Cancel job operation timed out.");
    }
  }

  /**
   * Validate expected state.
   *
   * <p>return true if validate.
   */
  abstract boolean validate(FlintIndexState state);

  /** get transitioningState */
  abstract FlintIndexState transitioningState();

  abstract void runOp(FlintIndexMetadata flintIndexMetadata, FlintIndexStateModel flintIndex);

  /** get stableState */
  abstract FlintIndexState stableState();
}
