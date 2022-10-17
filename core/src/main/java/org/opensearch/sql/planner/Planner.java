/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.streaming.time.RecordTimestampAssigner;
import org.opensearch.sql.planner.streaming.watermark.BoundedOutOfOrderWatermarkGenerator;
import org.opensearch.sql.storage.Table;

/**
 * Planner that plans and chooses the optimal physical plan.
 */
@RequiredArgsConstructor
public class Planner {

  private final LogicalPlanOptimizer logicalOptimizer;

  /**
   * Generate optimal physical plan for logical plan. If no table involved,
   * translate logical plan to physical by default implementor.
   * TODO: for now just delegate entire logical plan to storage engine.
   *
   * @param plan logical plan
   * @return optimal physical plan
   */
  public PhysicalPlan plan(LogicalPlan plan) {
    List<Table> tables = findTable(plan);
    Table writeTable = tables.get(1);
    Table sourceTable = tables.get(0);

    if (sourceTable == null && writeTable == null) {
      return plan.accept(new DefaultImplementor<>(), null);
    }

    if (sourceTable == null) {
      // values case, no sourceTable.
      return writeTable.implement(writeTable.optimize(plan));
    } else if (writeTable == null) {
      // DQL
      return sourceTable.implement(sourceTable.optimize(plan));
    } else {
      // DML
      return sourceTable.optimize(plan).accept(new WriteReadImplementor(writeTable, sourceTable),
          Optional.empty());
    }
  }

  private List<Table> findTable(LogicalPlan plan) {
    // 0: sourceTable, 1: writeTable
    List<Table> ctx = new ArrayList<>(2);
    ctx.add(0, null);
    ctx.add(1, null);

    plan.accept(new LogicalPlanNodeVisitor<Void, List<Table>>() {
      @Override
      public Void visitNode(LogicalPlan plan, List<Table> context) {
        List<LogicalPlan> children = plan.getChild();
        if (children.isEmpty()) {
          return null;
        }
        return children.get(0).accept(this, context);
      }

      @Override
      public Void visitWrite(LogicalWrite plan, List<Table> context) {
        context.set(1, plan.getTable());
        return super.visitWrite(plan, context);
      }

      @Override
      public Void visitRelation(LogicalRelation plan, List<Table> context) {
        context.set(0, plan.getTable());
        return null;
      }
    }, ctx);
    return ctx;
  }

  private String findTableName(LogicalPlan plan) {
    return plan.accept(new LogicalPlanNodeVisitor<String, Object>() {

      @Override
      public String visitNode(LogicalPlan node, Object context) {
        List<LogicalPlan> children = node.getChild();
        if (children.isEmpty()) {
          return "";
        }
        return children.get(0).accept(this, context);
      }

      @Override
      public String visitWrite(LogicalWrite plan, Object context) {
        String tableName = plan.getChild().get(0).accept(this, context);

        // Use tableName in write for Write(Values())
        // Use tableName in relation otherwise, ex. Write(Proj...(Relation))
        return (tableName == null || tableName.isEmpty()) ? plan.getTableName() : tableName;
      }

      @Override
      public String visitRelation(LogicalRelation node, Object context) {
        return node.getRelationName();
      }
    }, null);
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    return logicalOptimizer.optimize(plan);
  }

  @RequiredArgsConstructor
  private static class WriteReadImplementor extends DefaultImplementor<Optional<SpanExpression>> {

    private final Table writeTable;
    private final Table sourceTable;

    @Override
    public PhysicalPlan visitAggregation(LogicalAggregation node, Optional<SpanExpression> ctx) {
      List<NamedExpression> groupByList = node.getGroupByList();
      if (!groupByList.isEmpty()
          && groupByList.get(0).getDelegated() instanceof SpanExpression) {
        ctx = Optional.of((SpanExpression) groupByList.get(0).getDelegated());
      }

      final PhysicalPlan child = node.getChild().get(0).accept(this, ctx);
      return new AggregationOperator(child, node.getAggregatorList(), groupByList);
    }

    @Override
    public PhysicalPlan visitWrite(LogicalWrite node, Optional<SpanExpression> ctx) {
      final PhysicalPlan child = node.getChild().get(0).accept(this, ctx);
      return writeTable.implement(node, child);
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation plan, Optional<SpanExpression> ctx) {
      PhysicalPlan source = sourceTable.implement(plan);
      if (ctx.isPresent()) {
        source = new BoundedOutOfOrderWatermarkGenerator(
            source,
            new RecordTimestampAssigner(ctx.get().getField()),
            1000); // TODO: hardcoding latency allowed
      }
      return source;
    }
  }
}
