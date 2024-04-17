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
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

@ExtendWith(MockitoExtension.class)
public class FlintIndexOpTest {

  @Mock private FlintIndexStateModelService flintIndexStateModelService;

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
    when(flintIndexStateModelService.getFlintIndexStateModel(eq("latestId"), eq("myS3")))
        .thenReturn(Optional.of(fakeModel));
    when(flintIndexStateModelService.updateFlintIndexState(any(), any(), any()))
        .thenThrow(new RuntimeException("Transitioning state failed"));
    FlintIndexOp flintIndexOp = new TestFlintIndexOp(flintIndexStateModelService, "myS3");
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
    when(flintIndexStateModelService.getFlintIndexStateModel(eq("latestId"), eq("myS3")))
        .thenReturn(Optional.of(fakeModel));
    when(flintIndexStateModelService.updateFlintIndexState(any(), any(), any()))
        .thenReturn(FlintIndexStateModel.copy(fakeModel, 1, 2))
        .thenThrow(new RuntimeException("Commit state failed"))
        .thenReturn(FlintIndexStateModel.copy(fakeModel, 1, 3));
    FlintIndexOp flintIndexOp = new TestFlintIndexOp(flintIndexStateModelService, "myS3");
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
    when(flintIndexStateModelService.getFlintIndexStateModel(eq("latestId"), eq("myS3")))
        .thenReturn(Optional.of(fakeModel));
    when(flintIndexStateModelService.updateFlintIndexState(any(), any(), any()))
        .thenReturn(FlintIndexStateModel.copy(fakeModel, 1, 2))
        .thenThrow(new RuntimeException("Commit state failed"))
        .thenThrow(new RuntimeException("Rollback failure"));
    FlintIndexOp flintIndexOp = new TestFlintIndexOp(flintIndexStateModelService, "myS3");
    IllegalStateException illegalStateException =
        Assertions.assertThrows(IllegalStateException.class, () -> flintIndexOp.apply(metadata));
    Assertions.assertEquals(
        "commit failed. target stable state: [DELETED]", illegalStateException.getMessage());
  }

  static class TestFlintIndexOp extends FlintIndexOp {

    public TestFlintIndexOp(
        FlintIndexStateModelService flintIndexStateModelService, String datasourceName) {
      super(flintIndexStateModelService, datasourceName);
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
