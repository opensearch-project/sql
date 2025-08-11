/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.sql.calcite.plan.LogicalSystemLimit;

/**
 * Optimization rule that eliminates redundant consecutive sorts on the same field.
 * Detects: LogicalSort(field, direction1) -> LogicalSort(field, direction2)
 * Converts to: LogicalSort(field, direction1) (keeps outer sort)
 */
public class SortReverseOptimizationRule extends RelOptRule {

  public static final SortReverseOptimizationRule INSTANCE = new SortReverseOptimizationRule();

  private SortReverseOptimizationRule() {
    super(operand(LogicalSort.class,
        operand(LogicalSort.class, any())),
        "SortReverseOptimizationRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalSort outerSort = call.rel(0);
    LogicalSort innerSort = call.rel(1);
    
    // Don't optimize if outer sort is a LogicalSystemLimit - we want to preserve system limits
    if (call.rel(0) instanceof LogicalSystemLimit) {
      return false;
    }
    
    return hasSameField(outerSort, innerSort);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalSort outerSort = call.rel(0);
    LogicalSort innerSort = call.rel(1);
    
    LogicalSort optimizedSort = LogicalSort.create(
        innerSort.getInput(),
        outerSort.getCollation(),
        outerSort.offset,
        outerSort.fetch);
    
    call.transformTo(optimizedSort);
  }
  
  private boolean hasSameField(LogicalSort outerSort, LogicalSort innerSort) {
    if (outerSort.getCollation().getFieldCollations().isEmpty() 
        || innerSort.getCollation().getFieldCollations().isEmpty()) {
      return false;
    }
    
    int outerField = outerSort.getCollation().getFieldCollations().get(0).getFieldIndex();
    int innerField = innerSort.getCollation().getFieldCollations().get(0).getFieldIndex();
    return outerField == innerField;
  }
}