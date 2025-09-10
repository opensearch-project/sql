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

/** Combines consecutive sorts with opposite directions into 1 sort. */
public class SortDirectionOptRule extends RelOptRule {
  private static final Logger LOG = LogManager.getLogger(SortDirectionOptRule.class);

  public static final SortDirectionOptRule INSTANCE = new SortDirectionOptRule();

  private SortDirectionOptRule() {
    super(
        operand(LogicalSort.class, 
            operand(org.apache.calcite.rel.RelNode.class, 
                operand(LogicalSort.class, any()))),
        "SortDirectionOptRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalSort outerSort = call.rel(0);
    org.apache.calcite.rel.RelNode intermediate = call.rel(1);
    LogicalSort innerSort = call.rel(2);

    LOG.debug("SortDirectionOptRule.matches() - outer: {}, intermediate: {}, inner: {}", 
        outerSort, intermediate, innerSort);
        
    // Only allow single-input intermediate nodes (like LogicalProject)
    if (intermediate.getInputs().size() != 1) {
      LOG.debug("Intermediate node has {} inputs, expected 1", intermediate.getInputs().size());
      return false;
    }

    // Don't optimize if inner sort has a fetch limit (head/limit before sort)
    // This preserves limit-then-sort semantics
    // Example: source=t | head 5 | sort field | reverse
    // Plan: Sort(reverse) -> Sort(field, fetch=5) -> Scan
    // Should NOT be optimized to preserve the "take first 5, then sort" behavior
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
    org.apache.calcite.rel.RelNode intermediate = call.rel(1);
    LogicalSort innerSort = call.rel(2);

    LOG.debug("SortDirectionOptRule.onMatch() transforming: {} -> {}", outerSort, innerSort);

    // Create optimized sort with the final direction
    LogicalSort optimizedSort =
        LogicalSort.create(
            innerSort.getInput(), outerSort.getCollation(), outerSort.offset, outerSort.fetch);

    // Recreate the intermediate node with the optimized sort as input
    org.apache.calcite.rel.RelNode newIntermediate = 
        intermediate.copy(intermediate.getTraitSet(), 
            java.util.Collections.singletonList(optimizedSort));

    LOG.debug("Transformed to: {}", newIntermediate);
    call.transformTo(newIntermediate);
  }

  private boolean hasSameFieldWithOppositeDirection(LogicalSort outerSort, LogicalSort innerSort) {
    var outerFields = outerSort.getCollation().getFieldCollations();
    var innerFields = innerSort.getCollation().getFieldCollations();

    if (outerFields.isEmpty() || innerFields.isEmpty()) {
      LOG.debug("No field collations found");
      return false;
    }

    // Must have same number of fields
    if (outerFields.size() != innerFields.size()) {
      LOG.debug(
          "Different number of sort fields: outer={}, inner={}",
          outerFields.size(),
          innerFields.size());
      return false;
    }

    // Check all fields have same index but opposite directions
    for (int i = 0; i < outerFields.size(); i++) {
      var outerField = outerFields.get(i);
      var innerField = innerFields.get(i);

      LOG.debug(
          "Field {}: outer(index={}, direction={}), inner(index={}, direction={})",
          i,
          outerField.getFieldIndex(),
          outerField.getDirection(),
          innerField.getFieldIndex(),
          innerField.getDirection());

      if (outerField.getFieldIndex() != innerField.getFieldIndex()
          || outerField.getDirection() == innerField.getDirection()) {
        LOG.debug(
            "Field {} mismatch: same index={}, opposite direction={}",
            i,
            outerField.getFieldIndex() == innerField.getFieldIndex(),
            outerField.getDirection() != innerField.getDirection());
        return false;
      }
    }

    LOG.debug("All fields match with opposite directions");
    return true;
  }

}
