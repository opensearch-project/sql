package org.opensearch.sql.spark.flint.operation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

@ExtendWith(MockitoExtension.class)
public class FlintIndexOpTest {

  @Mock private StateStore mockStateStore;
  @Mock private EMRServerlessClientFactory mockEmrServerlessClientFactory;

  @Test
  public void testApplyWithTransitioningStateFailure() {
    FlintIndexMetadata metadata = mock(FlintIndexMetadata.class);
    when(metadata.getLatestId()).thenReturn(Optional.of("latestId"));
    FlintIndexStateModel fakeModel =
        new FlintIndexStateModel(
            FlintIndexState.ACTIVE,
            metadata.getAppId(),
            metadata.getJobId(),
            "latestId",
            "myS3",
            System.currentTimeMillis(),
            "",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    when(mockStateStore.get(eq("latestId"), any(), eq(DATASOURCE_TO_REQUEST_INDEX.apply("myS3"))))
        .thenReturn(Optional.of(fakeModel));
    when(mockStateStore.updateState(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Transitioning state failed"));
    FlintIndexOp flintIndexOp =
        new TestFlintIndexOp(mockStateStore, "myS3", mockEmrServerlessClientFactory);
    IllegalStateException illegalStateException =
        Assertions.assertThrows(IllegalStateException.class, () -> flintIndexOp.apply(metadata));
    Assertions.assertEquals(
        "Moving to transition state:DELETING failed.", illegalStateException.getMessage());
  }

  @Test
  public void testApplyWithCommitFailure() {
    FlintIndexMetadata metadata = mock(FlintIndexMetadata.class);
    when(metadata.getLatestId()).thenReturn(Optional.of("latestId"));
    FlintIndexStateModel fakeModel =
        new FlintIndexStateModel(
            FlintIndexState.ACTIVE,
            metadata.getAppId(),
            metadata.getJobId(),
            "latestId",
            "myS3",
            System.currentTimeMillis(),
            "",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    when(mockStateStore.get(eq("latestId"), any(), eq(DATASOURCE_TO_REQUEST_INDEX.apply("myS3"))))
        .thenReturn(Optional.of(fakeModel));
    when(mockStateStore.updateState(any(), any(), any(), any()))
        .thenReturn(FlintIndexStateModel.copy(fakeModel, 1, 2))
        .thenThrow(new RuntimeException("Commit state failed"))
        .thenReturn(FlintIndexStateModel.copy(fakeModel, 1, 3));
    FlintIndexOp flintIndexOp =
        new TestFlintIndexOp(mockStateStore, "myS3", mockEmrServerlessClientFactory);
    IllegalStateException illegalStateException =
        Assertions.assertThrows(IllegalStateException.class, () -> flintIndexOp.apply(metadata));
    Assertions.assertEquals(
        "commit failed. target stable state: [DELETED]", illegalStateException.getMessage());
  }

  @Test
  public void testApplyWithRollBackFailure() {
    FlintIndexMetadata metadata = mock(FlintIndexMetadata.class);
    when(metadata.getLatestId()).thenReturn(Optional.of("latestId"));
    FlintIndexStateModel fakeModel =
        new FlintIndexStateModel(
            FlintIndexState.ACTIVE,
            metadata.getAppId(),
            metadata.getJobId(),
            "latestId",
            "myS3",
            System.currentTimeMillis(),
            "",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    when(mockStateStore.get(eq("latestId"), any(), eq(DATASOURCE_TO_REQUEST_INDEX.apply("myS3"))))
        .thenReturn(Optional.of(fakeModel));
    when(mockStateStore.updateState(any(), any(), any(), any()))
        .thenReturn(FlintIndexStateModel.copy(fakeModel, 1, 2))
        .thenThrow(new RuntimeException("Commit state failed"))
        .thenThrow(new RuntimeException("Rollback failure"));
    FlintIndexOp flintIndexOp =
        new TestFlintIndexOp(mockStateStore, "myS3", mockEmrServerlessClientFactory);
    IllegalStateException illegalStateException =
        Assertions.assertThrows(IllegalStateException.class, () -> flintIndexOp.apply(metadata));
    Assertions.assertEquals(
        "commit failed. target stable state: [DELETED]", illegalStateException.getMessage());
  }

  static class TestFlintIndexOp extends FlintIndexOp {

    public TestFlintIndexOp(
        StateStore stateStore,
        String datasourceName,
        EMRServerlessClientFactory emrServerlessClientFactory) {
      super(stateStore, datasourceName, emrServerlessClientFactory);
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
    void runOp(FlintIndexMetadata flintIndexMetadata, FlintIndexStateModel flintIndex) {}

    @Override
    FlintIndexState stableState() {
      return FlintIndexState.DELETED;
    }
  }
}
