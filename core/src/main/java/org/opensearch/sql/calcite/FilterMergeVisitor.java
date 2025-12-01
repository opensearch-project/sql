/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/**
 * A RelNode visitor that merges consecutive LogicalFilter nodes into a single filter with combined
 * AND conditions. This prevents deep Filter RelNode chains that cause memory exhaustion (OOM) with
 * multiple filter operations.
 *
 * <p>Example transformation:
 *
 * <pre>
 * BEFORE:
 *   LogicalFilter(age > 30)
 *     LogicalFilter(age < 40)
 *       LogicalFilter(balance > 10000)
 *         TableScan
 *
 * AFTER:
 *   LogicalFilter(AND(age > 30, age < 40, balance > 10000))
 *     TableScan
 * </pre>
 *
 * This is a post-processing optimization that runs after the RelNode tree is constructed by
 * CalciteRelNodeVisitor.
 */
public class FilterMergeVisitor extends RelShuttleImpl {

  /**
   * Visits a LogicalFilter node and merges it with consecutive child LogicalFilter nodes.
   *
   * @param filter the LogicalFilter node to visit
   * @return the merged filter or the original filter if no merging is needed
   */
  @Override
  public RelNode visit(LogicalFilter filter) {
    RelNode newInput = filter.getInput().accept(this);

    List<RexNode> conditions = new ArrayList<>();
    conditions.add(filter.getCondition());

    RelNode current = newInput;
    while (current instanceof LogicalFilter) {
      LogicalFilter childFilter = (LogicalFilter) current;
      conditions.add(childFilter.getCondition());
      current = childFilter.getInput();
    }

    // If we collected multiple conditions, merge them
    if (conditions.size() > 1) {
      RelOptCluster cluster = filter.getCluster();
      RexBuilder rexBuilder = cluster.getRexBuilder();

      // Combine all conditions with AND
      RexNode combinedCondition =
          rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.AND, conditions);

      // Simplify the combined condition (e.g., remove redundant TRUE, optimize)
      combinedCondition = org.apache.calcite.rex.RexUtil.simplify(rexBuilder, combinedCondition);

      // Create a new filter with the combined condition
      return LogicalFilter.create(current, combinedCondition);
    }

    if (newInput != filter.getInput()) {
      return filter.copy(filter.getTraitSet(), newInput, filter.getCondition());
    }

    return filter;
  }
}
