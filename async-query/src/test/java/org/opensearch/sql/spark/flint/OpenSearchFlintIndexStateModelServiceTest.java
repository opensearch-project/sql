/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.xcontent.FlintIndexStateModelXContentSerializer;

@ExtendWith(MockitoExtension.class)
public class OpenSearchFlintIndexStateModelServiceTest {

  public static final String DATASOURCE = "DATASOURCE";
  public static final String ID = "ID";

  @Mock StateStore mockStateStore;
  @Mock FlintIndexStateModel flintIndexStateModel;
  @Mock FlintIndexState flintIndexState;
  @Mock FlintIndexStateModel responseFlintIndexStateModel;
  @Mock FlintIndexStateModelXContentSerializer flintIndexStateModelXContentSerializer;
  @Mock AsyncQueryRequestContext asyncQueryRequestContext;

  @InjectMocks OpenSearchFlintIndexStateModelService openSearchFlintIndexStateModelService;

  @Test
  void updateFlintIndexState() {
    when(mockStateStore.updateState(any(), any(), any(), any()))
        .thenReturn(responseFlintIndexStateModel);

    FlintIndexStateModel result =
        openSearchFlintIndexStateModelService.updateFlintIndexState(
            flintIndexStateModel, flintIndexState, DATASOURCE, asyncQueryRequestContext);

    assertEquals(responseFlintIndexStateModel, result);
  }

  @Test
  void getFlintIndexStateModel() {
    when(mockStateStore.get(any(), any(), any()))
        .thenReturn(Optional.of(responseFlintIndexStateModel));

    Optional<FlintIndexStateModel> result =
        openSearchFlintIndexStateModelService.getFlintIndexStateModel(
            "ID", DATASOURCE, asyncQueryRequestContext);

    assertEquals(responseFlintIndexStateModel, result.get());
  }

  @Test
  void createFlintIndexStateModel() {
    when(mockStateStore.create(any(), any(), any(), any()))
        .thenReturn(responseFlintIndexStateModel);
    when(flintIndexStateModel.getDatasourceName()).thenReturn(DATASOURCE);

    FlintIndexStateModel result =
        openSearchFlintIndexStateModelService.createFlintIndexStateModel(
            flintIndexStateModel, asyncQueryRequestContext);

    assertEquals(responseFlintIndexStateModel, result);
  }

  @Test
  void deleteFlintIndexStateModel() {
    when(mockStateStore.delete(any(), any())).thenReturn(true);

    boolean result =
        openSearchFlintIndexStateModelService.deleteFlintIndexStateModel(
            ID, DATASOURCE, asyncQueryRequestContext);

    assertTrue(result);
  }
}
