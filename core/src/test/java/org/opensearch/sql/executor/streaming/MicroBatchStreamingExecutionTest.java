/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
class MicroBatchStreamingExecutionTest {

  @Test
  void executedSuccess() {
    streamingQuery()
        .addData()
        .executeSuccess(0L)
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L);
  }

  @Test
  void executedFailed() {
    streamingQuery().addData().executeFailed().latestOffsetLogShouldBe(0L).noCommittedLog();
  }

  @Test
  void noDataInSource() {
    streamingQuery().neverProcess().noOffsetLog().noCommittedLog();
  }

  @Test
  void noNewDataInSource() {
    streamingQuery()
        .addData()
        .executeSuccess(0L)
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .neverProcess()
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L);
  }

  @Test
  void addNewDataInSequenceAllExecuteSuccess() {
    streamingQuery()
        .addData()
        .executeSuccess(0L)
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .addData()
        .executeSuccess(1L)
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(1L);
  }

  @Test
  void addNewDataInSequenceExecuteFailedInBetween() {
    streamingQuery()
        .addData()
        .executeSuccess(0L)
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .addData()
        .executeFailed()
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(0L)
        .executeSuccess(1L)
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(1L);
  }

  @Test
  void addNewDataInSequenceExecuteFailed() {
    streamingQuery()
        .addData()
        .executeSuccess(0L)
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .addData()
        .executeFailed()
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(0L)
        .executeFailed()
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(0L);
  }

  Helper streamingQuery() {
    return new Helper();
  }

  private static class Helper {

    private final MicroBatchStreamingExecution execution;

    private final MetadataLog<Offset> offsetLog;

    private final MetadataLog<Offset> committedLog;

    private final LogicalPlan batchPlan;

    private final QueryService queryService;

    private final TestStreamingSource source = new TestStreamingSource();

    public Helper() {
      this.offsetLog = new DefaultMetadataLog<>();
      this.committedLog = new DefaultMetadataLog<>();
      this.batchPlan = Mockito.mock(LogicalPlan.class);
      this.queryService = Mockito.mock(QueryService.class);
      this.execution =
          new MicroBatchStreamingExecution(
              source, batchPlan, queryService, offsetLog, committedLog);
    }

    Helper addData() {
      source.addData();
      return this;
    }

    Helper neverProcess() {
      lenient()
          .doAnswer(
              invocation -> {
                fail();
                return null;
              })
          .when(queryService)
          .executePlan(any(), any(), any());
      execution.execute();
      return this;
    }

    Helper executeSuccess(Long... offsets) {
      lenient()
          .doAnswer(
              invocation -> {
                ResponseListener<ExecutionEngine.QueryResponse> listener =
                    invocation.getArgument(2);
                listener.onResponse(
                    new ExecutionEngine.QueryResponse(null, Collections.emptyList(), Cursor.None));

                PlanContext planContext = invocation.getArgument(1);
                assertTrue(planContext.getSplit().isPresent());
                assertEquals(new TestOffsetSplit(offsets), planContext.getSplit().get());

                return null;
              })
          .when(queryService)
          .executePlan(any(), any(), any());
      execution.execute();

      return this;
    }

    Helper executeFailed() {
      lenient()
          .doAnswer(
              invocation -> {
                ResponseListener<ExecutionEngine.QueryResponse> listener =
                    invocation.getArgument(2);
                listener.onFailure(new RuntimeException());

                return null;
              })
          .when(queryService)
          .executePlan(any(), any(), any());
      execution.execute();

      return this;
    }

    Helper noCommittedLog() {
      assertTrue(committedLog.getLatest().isEmpty());
      return this;
    }

    Helper latestCommittedLogShouldBe(Long offsetId) {
      assertTrue(committedLog.getLatest().isPresent());
      assertEquals(offsetId, committedLog.getLatest().get().getRight().getOffset());
      return this;
    }

    Helper noOffsetLog() {
      assertTrue(offsetLog.getLatest().isEmpty());
      return this;
    }

    Helper latestOffsetLogShouldBe(Long offsetId) {
      assertTrue(offsetLog.getLatest().isPresent());
      assertEquals(offsetId, offsetLog.getLatest().get().getRight().getOffset());
      return this;
    }
  }

  /**
   * StreamingSource impl only for testing.
   *
   * <p>initially, offset is -1, getLatestOffset() will return Optional.emtpy().
   *
   * <p>call addData() add offset by one.
   */
  static class TestStreamingSource implements StreamingSource {

    private final AtomicLong offset = new AtomicLong(-1L);

    /** add offset by one. */
    void addData() {
      offset.incrementAndGet();
    }

    /** return offset if addData was called. */
    @Override
    public Optional<Offset> getLatestOffset() {
      if (offset.get() == -1) {
        return Optional.empty();
      } else {
        return Optional.of(new Offset(offset.get()));
      }
    }

    /** always return `empty` Batch regardless start and end offset. */
    @Override
    public Batch getBatch(Optional<Offset> start, Offset end) {
      return new Batch(
          new TestOffsetSplit(
              start.map(v -> v.getOffset() + 1).orElse(0L),
              Long.min(offset.get(), end.getOffset())));
    }
  }

  @EqualsAndHashCode
  static class TestOffsetSplit implements Split {

    private final List<Long> offsets;

    public TestOffsetSplit(Long start, Long end) {
      this.offsets = new ArrayList<>();
      for (long l = start; l <= end; l++) {
        this.offsets.add(l);
      }
    }

    public TestOffsetSplit(Long... offsets) {
      this.offsets = Arrays.stream(offsets).collect(Collectors.toList());
    }

    @Override
    public String getSplitId() {
      return "id";
    }
  }
}
