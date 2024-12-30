/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.xcontent.XContentSerializerUtil;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

@ExtendWith(MockitoExtension.class)
public class FlintIndexOpTest {

  @Mock private FlintIndexStateModelService flintIndexStateModelService;
  @Mock private EMRServerlessClientFactory mockEmrServerlessClientFactory;
  @Mock private AsyncQueryRequestContext asyncQueryRequestContext;

  @Test
  public void testApplyWithTransitioningStateFailure() {
    FlintIndexMetadata metadata = mock(FlintIndexMetadata.class);
    when(metadata.getLatestId()).thenReturn(Optional.of("latestId"));
    FlintIndexStateModel fakeModel = getFlintIndexStateModel(metadata);
    when(flintIndexStateModelService.getFlintIndexStateModel(
            eq("latestId"), any(), eq(asyncQueryRequestContext)))
        .thenReturn(Optional.of(fakeModel));
    when(flintIndexStateModelService.updateFlintIndexState(
            any(), any(), any(), eq(asyncQueryRequestContext)))
        .thenThrow(new RuntimeException("Transitioning state failed"));
    FlintIndexOp flintIndexOp =
        new TestFlintIndexOp(flintIndexStateModelService, "myS3", mockEmrServerlessClientFactory);

    IllegalStateException illegalStateException =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> flintIndexOp.apply(metadata, asyncQueryRequestContext));

    Assertions.assertEquals(
        "Moving to transition state:DELETING failed.", illegalStateException.getMessage());
  }

  @Test
  public void testApplyWithCommitFailure() {
    FlintIndexMetadata metadata = mock(FlintIndexMetadata.class);
    when(metadata.getLatestId()).thenReturn(Optional.of("latestId"));
    FlintIndexStateModel fakeModel = getFlintIndexStateModel(metadata);
    when(flintIndexStateModelService.getFlintIndexStateModel(
            eq("latestId"), any(), eq(asyncQueryRequestContext)))
        .thenReturn(Optional.of(fakeModel));
    when(flintIndexStateModelService.updateFlintIndexState(
            any(), any(), any(), eq(asyncQueryRequestContext)))
        .thenReturn(
            FlintIndexStateModel.copy(fakeModel, XContentSerializerUtil.buildMetadata(1, 2)))
        .thenThrow(new RuntimeException("Commit state failed"))
        .thenReturn(
            FlintIndexStateModel.copy(fakeModel, XContentSerializerUtil.buildMetadata(1, 3)));
    FlintIndexOp flintIndexOp =
        new TestFlintIndexOp(flintIndexStateModelService, "myS3", mockEmrServerlessClientFactory);

    IllegalStateException illegalStateException =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> flintIndexOp.apply(metadata, asyncQueryRequestContext));

    Assertions.assertEquals(
        "commit failed. target stable state: [DELETED]", illegalStateException.getMessage());
  }

  @Test
  public void testApplyWithRollBackFailure() {
    FlintIndexMetadata metadata = mock(FlintIndexMetadata.class);
    when(metadata.getLatestId()).thenReturn(Optional.of("latestId"));
    FlintIndexStateModel fakeModel = getFlintIndexStateModel(metadata);
    when(flintIndexStateModelService.getFlintIndexStateModel(
            eq("latestId"), any(), eq(asyncQueryRequestContext)))
        .thenReturn(Optional.of(fakeModel));
    when(flintIndexStateModelService.updateFlintIndexState(
            any(), any(), any(), eq(asyncQueryRequestContext)))
        .thenReturn(
            FlintIndexStateModel.copy(fakeModel, XContentSerializerUtil.buildMetadata(1, 2)))
        .thenThrow(new RuntimeException("Commit state failed"))
        .thenThrow(new RuntimeException("Rollback failure"));
    FlintIndexOp flintIndexOp =
        new TestFlintIndexOp(flintIndexStateModelService, "myS3", mockEmrServerlessClientFactory);

    IllegalStateException illegalStateException =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> flintIndexOp.apply(metadata, asyncQueryRequestContext));

    Assertions.assertEquals(
        "commit failed. target stable state: [DELETED]", illegalStateException.getMessage());
  }

  private FlintIndexStateModel getFlintIndexStateModel(FlintIndexMetadata metadata) {
    return FlintIndexStateModel.builder()
        .indexState(FlintIndexState.ACTIVE)
        .applicationId(metadata.getAppId())
        .jobId(metadata.getJobId())
        .latestId("latestId")
        .datasourceName("myS3")
        .lastUpdateTime(System.currentTimeMillis())
        .error("")
        .build();
  }

  static class TestFlintIndexOp extends FlintIndexOp {

    public TestFlintIndexOp(
        FlintIndexStateModelService flintIndexStateModelService,
        String datasourceName,
        EMRServerlessClientFactory emrServerlessClientFactory) {
      super(flintIndexStateModelService, datasourceName, emrServerlessClientFactory);
    }

    @Override
    boolean validate(FlintIndexState state) {
      return state == FlintIndexState.ACTIVE || state == FlintIndexState.EMPTY;
    }

    @Override
    FlintIndexState transitioningState() {
      return FlintIndexState.DELETING;
    }

    @Override
    void runOp(
        FlintIndexMetadata flintIndexMetadata,
        FlintIndexStateModel flintIndex,
        AsyncQueryRequestContext asyncQueryRequestContext) {}

    @Override
    FlintIndexState stableState() {
      return FlintIndexState.DELETED;
    }
  }
}
