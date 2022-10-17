/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stream;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.streaming.event.StreamContext;

public class MicroBatchTask {

  private static final Logger log = LogManager.getLogger(MicroBatchTask.class);

  private final StreamSource source;

  private final LogicalPlan batchPlan;

  private final QueryService queryService;

  private final MetadataLog<Offset> offsetLog;

  private final MetadataLog<Offset> committedLog;

  private final StreamContext streamContext = new StreamContext();

  public MicroBatchTask(StreamSource source, LogicalPlan batchPlan,
                        QueryService queryService) {
    this.source = source;
    this.batchPlan = batchPlan;
    this.queryService = queryService;
    this.offsetLog = new InMemoryMetadataLog<>();
    this.committedLog = new InMemoryMetadataLog<>();
  }


  public void execute() {
    Long latestBatchId = offsetLog.getLatest().map(Pair::getKey).orElse(-1L);

    Optional<Offset> availableOffset = offsetLog.get(latestBatchId);
    Optional<Offset> committedOffset = offsetLog.get(latestBatchId - 1);
    Long latestCommittedBatchId = committedLog.getLatest().map(Pair::getKey).orElse(-1L);
    final AtomicLong currentBatchId = new AtomicLong(-1L);

    log.info("latestBatchId={}, latestCommittedBatchId={}", latestBatchId, latestCommittedBatchId);
    if (latestBatchId.equals(latestCommittedBatchId)) {
      currentBatchId.set(latestCommittedBatchId + 1);
      committedOffset = availableOffset;
    } else if (latestBatchId.equals(latestCommittedBatchId + 1L)) {
      currentBatchId.set(latestBatchId);;
    } else {
      log.error("[ALERT] breaking loop invariant");
    }

    Optional<Offset> availableOffsets = source.getLatestOffset();
    if (hasNewData(availableOffsets, committedOffset)) {
      Batch batch = source.getBatch(committedOffset, availableOffsets.get());
      offsetLog.add(currentBatchId.get(), availableOffsets.get());
      PhysicalPlan newPlan = queryService.plan(rewriteRelation(batchPlan, batch));

      // Restore stream context from what's stored by previous batch
      StreamContextLookup streamContextLookup = new StreamContextLookup();
      newPlan.accept(streamContextLookup, null).copyFrom(streamContext);
      log.info("Restored stream context: {}", streamContext);

      queryService.executePlan(newPlan, new ResponseListener<>() {
        @Override
        public void onResponse(ExecutionEngine.QueryResponse response) {
          final long finalBatchId = currentBatchId.get();
          final Offset finalAvailableOffsets = availableOffsets.get();
          committedLog.add(finalBatchId, finalAvailableOffsets);
          streamContext.copyFrom(newPlan.accept(streamContextLookup, null));
        }

        @Override
        public void onFailure(Exception e) {
          log.error("");
        }
      });
    }
  }

  private LogicalPlan rewriteRelation(LogicalPlan root, Batch batch) {
    if (root instanceof LogicalRelation) {
      return new LogicalRelation(((LogicalRelation) root).getRelationName(), batch.toTable());
    } else {
      return root.replaceChildPlans(Arrays.asList(rewriteRelation(root.getChild().get(0), batch)));
    }
  }

  private boolean hasNewData(Optional<Offset> availableOffsets, Optional<Offset> committedOffset) {
    if (availableOffsets.equals(committedOffset)) {
      log.info("source: [{}] does not have new data, exit", source);
      return false;
    } else if (availableOffsets.isPresent()) {
      log.info("source: [{}] has new data. availableOffsets:{}, committedOffset:{}", source,
          availableOffsets, committedOffset);
      return true;
    } else {
      log.info("source: [{}] unexpected status. availableOffsets:{}, committedOffset:{}", source,
          availableOffsets, committedOffset);
      return false;
    }
  }

  private static class StreamContextLookup extends PhysicalPlanNodeVisitor<StreamContext, Object> {
    @Override
    protected StreamContext visitNode(PhysicalPlan node, Object context) {
      return node.getChild().get(0).accept(this, context);
    }

    @Override
    public StreamContext visitAggregation(AggregationOperator node, Object context) {
      return node.getStreamContext();
    }
  }
}
