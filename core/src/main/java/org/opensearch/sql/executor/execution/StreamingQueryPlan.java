/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.streaming.DefaultMetadataLog;
import org.opensearch.sql.executor.streaming.MicroBatchStreamingExecution;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;

/** Streaming Query Plan. */
public class StreamingQueryPlan extends QueryPlan {

  private static final Logger log = LogManager.getLogger(StreamingQueryPlan.class);

  private final ExecutionStrategy executionStrategy;

  private MicroBatchStreamingExecution streamingExecution;

  /** constructor. */
  public StreamingQueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ExecutionStrategy executionStrategy) {
    super(queryId, queryType, plan, queryService, listener);

    this.executionStrategy = executionStrategy;
  }

  @Override
  public void execute() {
    try {
      LogicalPlan logicalPlan = queryService.analyze(plan);
      StreamingSource streamingSource = buildStreamingSource(logicalPlan);
      streamingExecution =
          new MicroBatchStreamingExecution(
              streamingSource,
              logicalPlan,
              queryService,
              new DefaultMetadataLog<>(),
              new DefaultMetadataLog<>());
      executionStrategy.execute(streamingExecution::execute);
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      listener.onFailure(e);
    } catch (InterruptedException e) {
      log.error(e);
      // todo, update async task status.
    }
  }

  interface ExecutionStrategy {
    /** execute task. */
    void execute(Runnable task) throws InterruptedException;
  }

  /**
   * execute task with fixed interval.<br>
   * if task run time < interval, trigger next task on next interval.<br>
   * if task run time >= interval, trigger next task immediately.
   */
  @RequiredArgsConstructor
  public static class IntervalTriggerExecution implements ExecutionStrategy {

    private final long intervalInSeconds;

    @Override
    public void execute(Runnable runnable) throws InterruptedException {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Instant start = Instant.now();
          runnable.run();
          Instant end = Instant.now();
          long took = Duration.between(start, end).toSeconds();
          TimeUnit.SECONDS.sleep(intervalInSeconds > took ? intervalInSeconds - took : 0);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private StreamingSource buildStreamingSource(LogicalPlan logicalPlan) {
    return logicalPlan.accept(new StreamingSourceBuilder(), null);
  }

  static class StreamingSourceBuilder extends LogicalPlanNodeVisitor<StreamingSource, Void> {
    @Override
    public StreamingSource visitNode(LogicalPlan plan, Void context) {
      List<LogicalPlan> children = plan.getChild();
      if (children.isEmpty()) {
        String errorMsg =
            String.format(
                "Could find relation plan, %s does not have child node.",
                plan.getClass().getSimpleName());
        log.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
      }
      return children.get(0).accept(this, context);
    }

    @Override
    public StreamingSource visitRelation(LogicalRelation plan, Void context) {
      try {
        return plan.getTable().asStreamingSource();
      } catch (UnsupportedOperationException e) {
        String errorMsg =
            String.format(
                "table %s could not been used as streaming source.", plan.getRelationName());
        log.error(errorMsg);
        throw new UnsupportedOperationException(errorMsg);
      }
    }
  }
}
