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
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.seqno.SequenceNumbers;
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
      runOp(fakeModel);
    } else {
      Optional<FlintIndexStateModel> flintIndexOptional =
          getFlintIndexState(stateStore, datasourceName).apply(latestId.get());
      if (flintIndexOptional.isEmpty()) {
        String errorMsg = String.format(Locale.ROOT, "no state found. docId: %s", latestId.get());
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }
      FlintIndexStateModel flintIndex = flintIndexOptional.get();

      // 1.validate state.
      FlintIndexState currentState = flintIndex.getIndexState();
      if (!validate(currentState)) {
        String errorMsg =
            String.format(Locale.ROOT, "validate failed. unexpected state: [%s]", currentState);
        LOG.debug(errorMsg);
        return;
      }

      // 2.begin, move to transitioning state
      FlintIndexState transitioningState = transitioningState();
      try {
        flintIndex =
            updateFlintIndexState(stateStore, datasourceName)
                .apply(flintIndex, transitioningState());
      } catch (Exception e) {
        String errorMsg =
            String.format(
                Locale.ROOT, "begin failed. target transitioning state: [%s]", transitioningState);
        LOG.error(errorMsg, e);
        throw new IllegalStateException(errorMsg, e);
      }

      // 3.runOp
      runOp(flintIndex);

      // 4.commit, move to stable state
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
  }

  /**
   * Validate expected state.
   *
   * <p>return true if validate.
   */
  abstract boolean validate(FlintIndexState state);

  /** get transitioningState */
  abstract FlintIndexState transitioningState();

  abstract void runOp(FlintIndexStateModel flintIndex);

  /** get stableState */
  abstract FlintIndexState stableState();
}
