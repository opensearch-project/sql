/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
class MicroBatchStreamingExecutionTest {

  @Test
  void executedSuccess() {
    streamingQuery()
        .addData()
        .executeSuccess()
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L);
  }

  @Test
  void executedFailed() {
    streamingQuery().addData().executeFailed().latestOffsetLogShouldBe(0L).noCommittedLog();
  }

  @Test
  void noDataInSource() {
    streamingQuery().executeSuccess().noOffsetLog().noCommittedLog();
  }

  @Test
  void noNewDataInSource() {
    streamingQuery()
        .addData()
        .executeSuccess()
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .executeSuccess()
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L);
  }

  @Test
  void addNewDataInSequenceAllExecuteSuccess() {
    streamingQuery()
        .addData()
        .executeSuccess()
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .addData()
        .executeSuccess()
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(1L);
  }

  @Test
  void addNewDataInSequenceExecuteFailedInBetween() {
    streamingQuery()
        .addData()
        .executeSuccess()
        .latestOffsetLogShouldBe(0L)
        .latestCommittedLogShouldBe(0L)
        .addData()
        .executeFailed()
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(0L)
        .executeSuccess()
        .latestOffsetLogShouldBe(1L)
        .latestCommittedLogShouldBe(1L);
  }

  @Test
  void addNewDataInSequenceExecuteFailed() {
    streamingQuery()
        .addData()
        .executeSuccess()
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

    Helper executeSuccess() {
      lenient()
          .doAnswer(
              invocation -> {
                ResponseListener<ExecutionEngine.QueryResponse> listener =
                    invocation.getArgument(1);
                listener.onResponse(
                    new ExecutionEngine.QueryResponse(null, Collections.emptyList()));
                return null;
              })
          .when(queryService)
          .executePlan(any(), any());
      execution.execute();

      return this;
    }

    Helper executeFailed() {
      lenient()
          .doAnswer(
              invocation -> {
                ResponseListener<ExecutionEngine.QueryResponse> listener =
                    invocation.getArgument(1);
                listener.onFailure(new RuntimeException());
                return null;
              })
          .when(queryService)
          .executePlan(any(), any());
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
      return new Batch(() -> "id");
    }
  }
}
