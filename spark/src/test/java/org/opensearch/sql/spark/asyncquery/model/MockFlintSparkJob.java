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
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.OpenSearchFlintIndexStateModelService;

public class MockFlintSparkJob {
  private FlintIndexStateModel stateModel;
  private FlintIndexStateModelService flintIndexStateModelService;
  private String datasource;

  public MockFlintSparkJob(StateStore stateStore, String latestId, String datasource) {
    assertNotNull(latestId);
    this.flintIndexStateModelService = new OpenSearchFlintIndexStateModelService(stateStore);
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
    stateModel =
        this.flintIndexStateModelService.createFlintIndexStateModel(stateModel, datasource);
  }

  public void transition(FlintIndexState newState) {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(stateModel, newState, datasource);
  }

  public void refreshing() {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(
            stateModel, FlintIndexState.REFRESHING, datasource);
  }

  public void active() {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(
            stateModel, FlintIndexState.ACTIVE, datasource);
  }

  public void creating() {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(
            stateModel, FlintIndexState.CREATING, datasource);
  }

  public void updating() {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(
            stateModel, FlintIndexState.UPDATING, datasource);
  }

  public void deleting() {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(
            stateModel, FlintIndexState.DELETING, datasource);
  }

  public void deleted() {
    stateModel =
        this.flintIndexStateModelService.updateFlintIndexState(
            stateModel, FlintIndexState.DELETED, datasource);
  }

  public void assertState(FlintIndexState expected) {
    Optional<FlintIndexStateModel> stateModelOpt =
        this.flintIndexStateModelService.getFlintIndexStateModel(stateModel.getId(), datasource);
    assertTrue((stateModelOpt.isPresent()));
    assertEquals(expected, stateModelOpt.get().getIndexState());
  }
}
