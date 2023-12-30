/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceSpec.DATASOURCE;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

@ExtendWith(MockitoExtension.class)
class FlintIndexOpTest {
  @Mock private StateStore stateStore;

  @Mock private FlintIndexMetadata flintIndexMetadata;

  @Mock private FlintIndexStateModel model;

  @Test
  public void beginFailed() {
    when(stateStore.updateState(any(), any(), any(), any())).thenThrow(RuntimeException.class);
    when(stateStore.get(any(), any(), any())).thenReturn(Optional.of(model));
    when(model.getIndexState()).thenReturn(FlintIndexState.ACTIVE);
    when(flintIndexMetadata.getLatestId()).thenReturn(Optional.of("latestId"));

    FlintIndexOpDelete indexOp = new FlintIndexOpDelete(stateStore, DATASOURCE);
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> indexOp.apply(flintIndexMetadata));
    Assertions.assertEquals(
        "begin failed. target transitioning state: [DELETING]", exception.getMessage());
  }

  @Test
  public void commitFailed() {
    when(stateStore.updateState(any(), any(), any(), any()))
        .thenReturn(model)
        .thenThrow(RuntimeException.class);
    when(stateStore.get(any(), any(), any())).thenReturn(Optional.of(model));
    when(model.getIndexState()).thenReturn(FlintIndexState.EMPTY);
    when(flintIndexMetadata.getLatestId()).thenReturn(Optional.of("latestId"));

    FlintIndexOpDelete indexOp = new FlintIndexOpDelete(stateStore, DATASOURCE);
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> indexOp.apply(flintIndexMetadata));
    Assertions.assertEquals(
        "commit failed. target stable state: [DELETED]", exception.getMessage());
  }
}
