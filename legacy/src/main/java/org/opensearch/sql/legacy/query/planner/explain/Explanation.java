/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.explain;

import com.google.common.collect.ImmutableMap;
import org.opensearch.sql.legacy.query.planner.core.Plan;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.core.PlanNode.Visitor;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.logical.node.Group;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/** Base class for different explanation implementation */
public class Explanation implements Visitor {

  /** Hard coding description to be consistent with old nested join explanation */
  private static final String DESCRIPTION =
      "Hash Join algorithm builds hash table based on result of first query, "
          + "and then probes hash table to find matched rows for each row returned by second query";

  /** Plans to be explained */
  private final Plan logicalPlan;

  private final Plan physicalPlan;

  /** Explanation format */
  private final ExplanationFormat format;

  public Explanation(Plan logicalPlan, Plan physicalPlan, ExplanationFormat format) {
    this.logicalPlan = logicalPlan;
    this.physicalPlan = physicalPlan;
    this.format = format;
  }

  @Override
  public String toString() {
    format.prepare(ImmutableMap.of("description", DESCRIPTION));

    format.start("Logical Plan");
    logicalPlan.traverse(this);
    format.end();

    format.start("Physical Plan");
    physicalPlan.traverse(this);
    format.end();

    return format.toString();
  }

  @Override
  public boolean visit(PlanNode node) {
    if (isValidOp(node)) {
      format.explain(node);
    }
    return true;
  }

  @Override
  public void endVisit(PlanNode node) {
    if (isValidOp(node)) {
      format.end();
    }
  }

  /** Check if node is a valid logical or physical operator */
  private boolean isValidOp(PlanNode node) {
    return isValidLogical(node) || isPhysical(node);
  }

  /** Valid logical operator means it's Group OR NOT a no-op because Group clarify explanation */
  private boolean isValidLogical(PlanNode node) {
    return (node instanceof LogicalOperator)
        && (node instanceof Group || !((LogicalOperator) node).isNoOp());
  }

  /** Right now all physical operators are valid and non-no-op */
  private boolean isPhysical(PlanNode node) {
    return node instanceof PhysicalOperator;
  }
}
