/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.streaming;

import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * Micro batch streaming execution.
 */
public class MicroBatchStreamingExecution {

  private static final Logger log = LogManager.getLogger(MicroBatchStreamingExecution.class);

  static final long INITIAL_LATEST_BATCH_ID = -1L;

  private final StreamingSource source;

  private final LogicalPlan batchPlan;

  private final QueryService queryService;

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log before any
   * processing is done. Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  private final MetadataLog<Offset> offsetLog;

  /** keep track the latest commit batchId. */
  private final MetadataLog<Offset> committedLog;

  /**
   * Constructor.
   */
  public MicroBatchStreamingExecution(
      StreamingSource source,
      LogicalPlan batchPlan,
      QueryService queryService,
      MetadataLog<Offset> offsetLog,
      MetadataLog<Offset> committedLog) {
    this.source = source;
    this.batchPlan = batchPlan;
    this.queryService = queryService;
    // todo. add offsetLog and committedLog offset recovery.
    this.offsetLog = offsetLog;
    this.committedLog = committedLog;
  }

  /**
   * Pull the {@link Batch} from {@link StreamingSource} and execute the {@link Batch}.
   */
  public void execute() {
    Long latestBatchId = offsetLog.getLatest().map(Pair::getKey).orElse(INITIAL_LATEST_BATCH_ID);
    Long latestCommittedBatchId =
        committedLog.getLatest().map(Pair::getKey).orElse(INITIAL_LATEST_BATCH_ID);
    Optional<Offset> committedOffset = offsetLog.get(latestCommittedBatchId);
    AtomicLong currentBatchId = new AtomicLong(INITIAL_LATEST_BATCH_ID);

    if (latestBatchId.equals(latestCommittedBatchId)) {
      // there are no unhandled Offset.
      currentBatchId.set(latestCommittedBatchId + 1L);
    } else {
      Preconditions.checkArgument(
          latestBatchId.equals(latestCommittedBatchId + 1L),
          "[BUG] Expected latestBatchId - latestCommittedBatchId = 0 or 1, "
              + "but latestBatchId=%d, latestCommittedBatchId=%d",
          latestBatchId,
          latestCommittedBatchId);

      // latestBatchId is not committed yet.
      currentBatchId.set(latestBatchId);
    }

    Optional<Offset> availableOffsets = source.getLatestOffset();
    if (hasNewData(availableOffsets, committedOffset)) {
      Batch batch = source.getBatch(committedOffset, availableOffsets.get());
      offsetLog.add(currentBatchId.get(), availableOffsets.get());
      queryService.executePlan(
          batchPlan,
          new PlanContext(batch.getSplit()),
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse response) {
              long finalBatchId = currentBatchId.get();
              Offset finalAvailableOffsets = availableOffsets.get();
              committedLog.add(finalBatchId, finalAvailableOffsets);
            }

            @Override
            public void onFailure(Exception e) {
              log.error("streaming processing failed. source = {} {}", source, e);
            }
          });
    }
  }

  private boolean hasNewData(Optional<Offset> availableOffsets, Optional<Offset> committedOffset) {
    if (availableOffsets.equals(committedOffset)) {
      log.debug("source does not have new data, exit. source = {}", source);
      return false;
    } else {
      Preconditions.checkArgument(
          availableOffsets.isPresent(), "[BUG] available offsets must be no empty");

      log.debug(
          "source has new data. source = {}, availableOffsets:{}, committedOffset:{}",
          source,
          availableOffsets,
          committedOffset);
      return true;
    }
  }
}
