/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.plan.LogicalSystemLimit;

/** Combines sort then reverse into 1 sort. */
public class SortReverseOptimizationRule extends RelOptRule {
  private static final Logger LOG = LogManager.getLogger(SortReverseOptimizationRule.class);

  public static final SortReverseOptimizationRule INSTANCE = new SortReverseOptimizationRule();

  private SortReverseOptimizationRule() {
    super(
        operand(LogicalSort.class, operand(LogicalSort.class, any())),
        "SortReverseOptimizationRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalSort outerSort = call.rel(0);
    LogicalSort innerSort = call.rel(1);

    LOG.debug("SortReverseOptimizationRule.matches() called");
    LOG.debug("Outer sort: {}", outerSort);
    LOG.debug("Inner sort: {}", innerSort);
    LOG.debug("Inner sort input: {}", innerSort.getInput());

    // Don't optimize if outer sort is a LogicalSystemLimit
    if (call.rel(0) instanceof LogicalSystemLimit) {
      LOG.debug("Skipping: outer sort is LogicalSystemLimit");
      return false;
    }

    // Don't optimize if inner sort has a fetch limit (head/limit before sort)
    // This preserves limit-then-sort semantics
    if (innerSort.fetch != null) {
      LOG.debug("Skipping: inner sort has fetch limit: {}", innerSort.fetch);
      return false;
    }

    // Must be same field with opposite directions (sort | reverse pattern)
    boolean matches = hasSameFieldWithOppositeDirection(outerSort, innerSort);
    LOG.debug("Same field with opposite direction: {}", matches);
    return matches;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalSort outerSort = call.rel(0);
    LogicalSort innerSort = call.rel(1);

    LOG.debug("SortReverseOptimizationRule.onMatch() applying transformation");
    LOG.debug("Transforming from: {} -> {}", outerSort, innerSort);

    LogicalSort optimizedSort =
        LogicalSort.create(
            innerSort.getInput(), outerSort.getCollation(), outerSort.offset, outerSort.fetch);

    LOG.debug("Transformed to: {}", optimizedSort);
    call.transformTo(optimizedSort);
  }

  private boolean hasSameFieldWithOppositeDirection(LogicalSort outerSort, LogicalSort innerSort) {
    if (outerSort.getCollation().getFieldCollations().isEmpty()
        || innerSort.getCollation().getFieldCollations().isEmpty()) {
      LOG.debug("No field collations found");
      return false;
    }

    var outerField = outerSort.getCollation().getFieldCollations().get(0);
    var innerField = innerSort.getCollation().getFieldCollations().get(0);

    LOG.debug(
        "Outer field: index={}, direction={}",
        outerField.getFieldIndex(),
        outerField.getDirection());
    LOG.debug(
        "Inner field: index={}, direction={}",
        innerField.getFieldIndex(),
        innerField.getDirection());

    boolean sameField = outerField.getFieldIndex() == innerField.getFieldIndex();
    boolean oppositeDirection = outerField.getDirection() != innerField.getDirection();

    LOG.debug("Same field: {}, Opposite direction: {}", sameField, oppositeDirection);

    // Must be same field with opposite directions
    return sameField && oppositeDirection;
  }
}
