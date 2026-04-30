/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/** Planner that plans and chooses the optimal physical plan. */
@RequiredArgsConstructor
public class Planner {

  private final LogicalPlanOptimizer logicalOptimizer;

  /**
   * Generate optimal physical plan for logical plan. If no table involved, translate logical plan
   * to physical by default implementor.<br>
   * TODO: for now just delegate entire logical plan to storage engine.
   *
   * @param plan logical plan
   * @return optimal physical plan
   */
  public PhysicalPlan plan(LogicalPlan plan) {
    Table table = findTable(plan);
    if (table == null) {
      return plan.accept(new DefaultImplementor<>(), null);
    }
    LogicalPlan optimized = table.optimize(optimize(plan));
    // Give scan builders a chance to reject shapes that push-down alone cannot express safely
    // (e.g. operators that land above the scan but outside its push-down contract).
    validateScanBuilders(optimized);
    return table.implement(optimized);
  }

  /**
   * Walk the optimized plan and invoke {@link TableScanBuilder#validatePlan(LogicalPlan)} on every
   * scan builder, passing the fully optimized root so scan builders can inspect their ancestors.
   */
  private void validateScanBuilders(LogicalPlan optimized) {
    optimized.accept(
        new LogicalPlanNodeVisitor<Void, Object>() {
          @Override
          public Void visitNode(LogicalPlan node, Object context) {
            for (LogicalPlan child : node.getChild()) {
              child.accept(this, context);
            }
            return null;
          }

          @Override
          public Void visitTableScanBuilder(TableScanBuilder node, Object context) {
            node.validatePlan(optimized);
            return null;
          }
        },
        null);
  }

  private Table findTable(LogicalPlan plan) {
    return plan.accept(
        new LogicalPlanNodeVisitor<Table, Object>() {

          @Override
          public Table visitNode(LogicalPlan node, Object context) {
            List<LogicalPlan> children = node.getChild();
            if (children.isEmpty()) {
              return null;
            }
            return children.get(0).accept(this, context);
          }

          @Override
          public Table visitRelation(LogicalRelation node, Object context) {
            return node.getTable();
          }
        },
        null);
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    return logicalOptimizer.optimize(plan);
  }

  public RelNode customOptimize(RelNode plan) {
    return logicalOptimizer.customOptimize(plan);
  }
}
