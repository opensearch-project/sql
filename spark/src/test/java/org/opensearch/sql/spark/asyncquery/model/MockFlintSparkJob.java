/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

public class MockFlintSparkJob {
  private FlintIndexStateModel stateModel;
  private FlintIndexStateModelService flintIndexStateModelService;
  private String datasource;

  public MockFlintSparkJob(
      FlintIndexStateModelService flintIndexStateModelService, String latestId, String datasource) {
    assertNotNull(latestId);
    this.flintIndexStateModelService = flintIndexStateModelService;
    this.datasource = datasource;
    stateModel =
        FlintIndexStateModel.builder()
            .indexState(FlintIndexState.EMPTY)
            .applicationId("mockAppId")
            .jobId("mockJobId")
            .latestId(latestId)
            .datasourceName(datasource)
            .lastUpdateTime(System.currentTimeMillis())
            .error("")
            .build();
    stateModel = flintIndexStateModelService.createFlintIndexStateModel(stateModel);
  }

  public void transition(FlintIndexState newState) {
    stateModel =
        flintIndexStateModelService.updateFlintIndexState(stateModel, newState, datasource);
  }

  public void refreshing() {
    transition(FlintIndexState.REFRESHING);
  }

  public void active() {
    transition(FlintIndexState.ACTIVE);
  }

  public void creating() {
    transition(FlintIndexState.CREATING);
  }

  public void updating() {
    transition(FlintIndexState.UPDATING);
  }

  public void deleting() {
    transition(FlintIndexState.DELETING);
  }

  public void deleted() {
    transition(FlintIndexState.DELETED);
  }

  public void assertState(FlintIndexState expected) {
    Optional<FlintIndexStateModel> stateModelOpt =
        flintIndexStateModelService.getFlintIndexStateModel(stateModel.getId(), datasource);
    assertTrue(stateModelOpt.isPresent());
    assertEquals(expected, stateModelOpt.get().getIndexState());
  }
}
