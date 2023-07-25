/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/** Planner that plans and chooses the optimal physical plan. */
@RequiredArgsConstructor
public class Planner {

  private final LogicalPlanOptimizer logicalOptimizer;

  /**
   * Generate optimal physical plan for logical plan. If no table involved, translate logical plan
   * to physical by default implementor. TODO: for now just delegate entire logical plan to storage
   * engine.
   *
   * @param plan logical plan
   * @return optimal physical plan
   */
  public PhysicalPlan plan(LogicalPlan plan) {
    Table table = findTable(plan);
    if (table == null) {
      return plan.accept(new DefaultImplementor<>(), null);
    }
    return table.implement(table.optimize(optimize(plan)));
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
}
