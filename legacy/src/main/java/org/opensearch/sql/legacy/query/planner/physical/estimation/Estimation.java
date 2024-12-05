/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.estimation;

import static java.util.Comparator.comparing;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.logical.LogicalPlanVisitor;
import org.opensearch.sql.legacy.query.planner.logical.node.Group;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/**
 * Convert and estimate the cost of each operator and generate one optimal plan. Memorize cost of
 * candidate physical operators in the bottom-up way to avoid duplicate computation.
 */
public class Estimation<T> implements LogicalPlanVisitor {

  /** Optimal physical operator for logical operator based on completed estimation */
  private final Map<LogicalOperator, PhysicalOperator<T>> optimalOps = new IdentityHashMap<>();

  /** Keep tracking of the operator that exit visit() */
  private PhysicalOperator<T> root;

  @Override
  public boolean visit(Group group) {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void endVisit(PlanNode node) {
    LogicalOperator op = (LogicalOperator) node;
    PhysicalOperator<T> optimal =
        Arrays.stream(op.toPhysical(optimalOps))
            .min(comparing(PhysicalOperator::estimate))
            .orElseThrow(() -> new IllegalStateException("No optimal operator found: " + op));
    optimalOps.put(op, optimal);
    root = optimal;
  }

  public PhysicalOperator<T> optimalPlan() {
    return root;
  }
}
