/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import static org.opensearch.sql.spark.client.EmrServerlessClientImpl.GENERIC_INTERNAL_SERVER_ERROR_MESSAGE;

import com.amazonaws.services.emrserverless.model.ValidationException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

/** Flint Index Operation. */
@RequiredArgsConstructor
public abstract class FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final FlintIndexStateModelService flintIndexStateModelService;
  private final String datasourceName;
  private final EMRServerlessClientFactory emrServerlessClientFactory;

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
          flintIndexStateModelService.updateFlintIndexState(
              transitionedFlintIndexStateModel,
              initialFlintIndexStateModel.getIndexState(),
              datasourceName);
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
        flintIndexStateModelService.getFlintIndexStateModel(latestId, datasourceName);
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
        FlintIndexStateModel.builder()
            .indexState(FlintIndexState.REFRESHING)
            .applicationId(metadata.getAppId())
            .jobId(metadata.getJobId())
            .latestId("")
            .datasourceName(datasourceName)
            .lastUpdateTime(System.currentTimeMillis())
            .error("")
            .build();
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
          flintIndexStateModelService.updateFlintIndexState(
              flintIndex, transitioningState(), datasourceName);
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
        flintIndexStateModelService.deleteFlintIndexStateModel(
            flintIndex.getLatestId(), datasourceName);
      } else {
        flintIndexStateModelService.updateFlintIndexState(flintIndex, stableState, datasourceName);
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
  public void cancelStreamingJob(FlintIndexStateModel flintIndexStateModel)
      throws InterruptedException, TimeoutException {
    String applicationId = flintIndexStateModel.getApplicationId();
    String jobId = flintIndexStateModel.getJobId();
    EMRServerlessClient emrServerlessClient =
        emrServerlessClientFactory.getClient(flintIndexStateModel.getAccountId());
    try {
      emrServerlessClient.cancelJobRun(
          flintIndexStateModel.getApplicationId(), flintIndexStateModel.getJobId(), true);
    } catch (ValidationException e) {
      // Exception when the job is not in cancellable state and already in terminal state.
      if (e.getMessage().contains("Job run is not in a cancellable state")) {
        LOG.error(e);
        return;
      } else {
        throw new RuntimeException(GENERIC_INTERNAL_SERVER_ERROR_MESSAGE);
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(GENERIC_INTERNAL_SERVER_ERROR_MESSAGE);
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
