/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexClient;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

@ExtendWith(MockitoExtension.class)
class FlintIndexOpVacuumTest {

  public static final String DATASOURCE_NAME = "DATASOURCE_NAME";
  public static final String LATEST_ID = "LATEST_ID";
  public static final String INDEX_NAME = "INDEX_NAME";
  public static final FlintIndexMetadata FLINT_INDEX_METADATA_WITH_LATEST_ID =
      FlintIndexMetadata.builder().latestId(LATEST_ID).opensearchIndexName(INDEX_NAME).build();
  public static final FlintIndexMetadata FLINT_INDEX_METADATA_WITHOUT_LATEST_ID =
      FlintIndexMetadata.builder().opensearchIndexName(INDEX_NAME).build();
  @Mock FlintIndexClient flintIndexClient;
  @Mock FlintIndexStateModelService flintIndexStateModelService;
  @Mock EMRServerlessClientFactory emrServerlessClientFactory;
  @Mock FlintIndexStateModel flintIndexStateModel;
  @Mock FlintIndexStateModel transitionedFlintIndexStateModel;

  RuntimeException testException = new RuntimeException("Test Exception");

  FlintIndexOpVacuum flintIndexOpVacuum;

  @BeforeEach
  public void setUp() {
    flintIndexOpVacuum =
        new FlintIndexOpVacuum(
            flintIndexStateModelService,
            DATASOURCE_NAME,
            flintIndexClient,
            emrServerlessClientFactory);
  }

  @Test
  public void emptyLatestId_apply() {
    flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITHOUT_LATEST_ID);

    verify(flintIndexClient).deleteIndex(INDEX_NAME);
  }

  @Test
  public void flintIndexStateNotFound_apply_throw() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.empty());

    assertThrows(
        IllegalStateException.class,
        () -> flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID));
  }

  @Test
  public void notDeletedState_apply_throw() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(flintIndexStateModel));
    when(flintIndexStateModel.getIndexState()).thenReturn(FlintIndexState.ACTIVE);

    assertThrows(
        IllegalStateException.class,
        () -> flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID));
  }

  @Test
  public void updateFlintIndexStateThrow_apply_throw() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(flintIndexStateModel));
    when(flintIndexStateModel.getIndexState()).thenReturn(FlintIndexState.DELETED);
    when(flintIndexStateModelService.updateFlintIndexState(
            flintIndexStateModel, FlintIndexState.VACUUMING, DATASOURCE_NAME))
        .thenThrow(testException);

    assertThrows(
        IllegalStateException.class,
        () -> flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID));
  }

  @Test
  public void runOpThrow_apply_rollbackAndThrow() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(flintIndexStateModel));
    when(flintIndexStateModel.getIndexState()).thenReturn(FlintIndexState.DELETED);
    when(flintIndexStateModelService.updateFlintIndexState(
            flintIndexStateModel, FlintIndexState.VACUUMING, DATASOURCE_NAME))
        .thenReturn(transitionedFlintIndexStateModel);
    doThrow(testException).when(flintIndexClient).deleteIndex(INDEX_NAME);

    assertThrows(
        Exception.class, () -> flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID));

    verify(flintIndexStateModelService)
        .updateFlintIndexState(
            transitionedFlintIndexStateModel, FlintIndexState.DELETED, DATASOURCE_NAME);
  }

  @Test
  public void runOpThrowAndRollbackThrow_apply_throw() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(flintIndexStateModel));
    when(flintIndexStateModel.getIndexState()).thenReturn(FlintIndexState.DELETED);
    when(flintIndexStateModelService.updateFlintIndexState(
            flintIndexStateModel, FlintIndexState.VACUUMING, DATASOURCE_NAME))
        .thenReturn(transitionedFlintIndexStateModel);
    doThrow(testException).when(flintIndexClient).deleteIndex(INDEX_NAME);
    when(flintIndexStateModelService.updateFlintIndexState(
            transitionedFlintIndexStateModel, FlintIndexState.DELETED, DATASOURCE_NAME))
        .thenThrow(testException);

    assertThrows(
        Exception.class, () -> flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID));
  }

  @Test
  public void deleteFlintIndexStateModelThrow_apply_throw() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(flintIndexStateModel));
    when(flintIndexStateModel.getIndexState()).thenReturn(FlintIndexState.DELETED);
    when(flintIndexStateModelService.updateFlintIndexState(
            flintIndexStateModel, FlintIndexState.VACUUMING, DATASOURCE_NAME))
        .thenReturn(transitionedFlintIndexStateModel);
    when(flintIndexStateModelService.deleteFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenThrow(testException);

    assertThrows(
        IllegalStateException.class,
        () -> flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID));
  }

  @Test
  public void happyPath_apply() {
    when(flintIndexStateModelService.getFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME))
        .thenReturn(Optional.of(flintIndexStateModel));
    when(flintIndexStateModel.getIndexState()).thenReturn(FlintIndexState.DELETED);
    when(flintIndexStateModelService.updateFlintIndexState(
            flintIndexStateModel, FlintIndexState.VACUUMING, DATASOURCE_NAME))
        .thenReturn(transitionedFlintIndexStateModel);
    when(transitionedFlintIndexStateModel.getLatestId()).thenReturn(LATEST_ID);

    flintIndexOpVacuum.apply(FLINT_INDEX_METADATA_WITH_LATEST_ID);

    verify(flintIndexStateModelService).deleteFlintIndexStateModel(LATEST_ID, DATASOURCE_NAME);
    verify(flintIndexClient).deleteIndex(INDEX_NAME);
  }
}
