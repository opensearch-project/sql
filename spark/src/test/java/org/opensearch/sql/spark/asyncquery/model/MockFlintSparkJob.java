/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

public class MockFlintSparkJob {
  private FlintIndexStateModel stateModel;
  private StateStore stateStore;
  private String datasource;

  public MockFlintSparkJob(StateStore stateStore, String latestId, String datasource) {
    assertNotNull(latestId);
    this.stateStore = stateStore;
    this.datasource = datasource;
    stateModel =
        new FlintIndexStateModel(
            FlintIndexState.EMPTY,
            "mockAppId",
            "mockJobId",
            latestId,
            datasource,
            System.currentTimeMillis(),
            "",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    stateModel = StateStore.createFlintIndexState(stateStore, datasource).apply(stateModel);
  }

  public void transition(FlintIndexState newState) {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, newState);
  }

  public void refreshing() {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, FlintIndexState.REFRESHING);
  }

  public void active() {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, FlintIndexState.ACTIVE);
  }

  public void creating() {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, FlintIndexState.CREATING);
  }

  public void updating() {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, FlintIndexState.UPDATING);
  }

  public void deleting() {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, FlintIndexState.DELETING);
  }

  public void deleted() {
    stateModel =
        StateStore.updateFlintIndexState(stateStore, datasource)
            .apply(stateModel, FlintIndexState.DELETED);
  }

  public void assertState(FlintIndexState expected) {
    Optional<FlintIndexStateModel> stateModelOpt =
        StateStore.getFlintIndexState(stateStore, datasource).apply(stateModel.getId());
    assertTrue((stateModelOpt.isPresent()));
    assertEquals(expected, stateModelOpt.get().getIndexState());
  }
}
