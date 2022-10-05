/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.stream.MicroBatchTask;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.storage.StreamTable;

public class StreamQueryExecution extends QueryExecution {
  /** The query plan ast. */
  private final UnresolvedPlan plan;

  /** True if the QueryExecution is explain only. */
  private final boolean isExplain;

  /** Query service. */
  private final QueryService queryService;

  private final MicroBatchTask task;

  /** Response listener. */
  private Optional<ResponseListener<?>> listener = Optional.empty();

  public StreamQueryExecution(
      QueryId queryId, UnresolvedPlan plan, boolean isExplain, QueryService queryService) {
    super(queryId);
    this.plan = plan;
    this.isExplain = isExplain;
    this.queryService = queryService;
    LogicalPlan logicalPlan = queryService.analyze(plan);
    LogicalRelation relation = (LogicalRelation) logicalPlan.accept(new RelationVisitor(), null);

    this.task =
        new MicroBatchTask(
            ((StreamTable) relation.getTable()).toStreamSource(), logicalPlan, queryService);
  }

  static class RelationVisitor extends LogicalPlanNodeVisitor<LogicalPlan, Void> {
    @Override
    public LogicalPlan visitNode(LogicalPlan plan, Void context) {
      List<LogicalPlan> children = plan.getChild();
      if (children.isEmpty()) {
        return null;
      }
      return children.get(0).accept(this, context);
    }

    @Override
    public LogicalPlan visitRelation(LogicalRelation plan, Void context) {
      return plan;
    }
  }

  @Override
  public void start() {
    int interval = 10;
    IntervalTriggerExecutor executor = new IntervalTriggerExecutor(interval);
    executor.execute(task::execute);
    listener.ifPresent(
        responseListener ->
            ((ResponseListener<ExecutionEngine.QueryResponse>) responseListener)
                .onResponse(
                    new ExecutionEngine.QueryResponse(
                        new ExecutionEngine.Schema(
                            Collections.singletonList(
                                new ExecutionEngine.Schema.Column(
                                    "queryId", "queryId", ExprCoreType.STRING))),
                        Collections.singletonList(
                            ExprValueUtils.tupleValue(
                                ImmutableMap.of("queryId", getQueryId().getQueryId()))))));
  }

  @Override
  public void registerListener(ResponseListener<?> listener) {
    this.listener = Optional.of(listener);
  }

  @RequiredArgsConstructor
  private static class IntervalTriggerExecutor {

    private final long intervalInSeconds;

    void execute(Runnable runnable) {
      while (true) {
        Instant start = Instant.now();
        runnable.run();
        Instant end = Instant.now();
        long took = Duration.between(start, end).toSeconds();
        try {
          TimeUnit.SECONDS.sleep(intervalInSeconds > took ? intervalInSeconds - took : 0);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
