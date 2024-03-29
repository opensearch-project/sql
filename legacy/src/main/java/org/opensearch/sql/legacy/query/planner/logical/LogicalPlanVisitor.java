/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical;

import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.core.PlanNode.Visitor;
import org.opensearch.sql.legacy.query.planner.logical.node.Filter;
import org.opensearch.sql.legacy.query.planner.logical.node.Group;
import org.opensearch.sql.legacy.query.planner.logical.node.Join;
import org.opensearch.sql.legacy.query.planner.logical.node.Project;
import org.opensearch.sql.legacy.query.planner.logical.node.Sort;
import org.opensearch.sql.legacy.query.planner.logical.node.TableScan;
import org.opensearch.sql.legacy.query.planner.logical.node.Top;

/**
 * Transformation rule for logical plan tree optimization implemented by standard Visitor pattern.
 */
public interface LogicalPlanVisitor extends Visitor {

  @Override
  default boolean visit(PlanNode op) {
    if (op instanceof Project) {
      return visit((Project) op);
    } else if (op instanceof Filter) {
      return visit((Filter) op);
    } else if (op instanceof Join) {
      return visit((Join) op);
    } else if (op instanceof Group) {
      return visit((Group) op);
    } else if (op instanceof TableScan) {
      return visit((TableScan) op);
    } else if (op instanceof Top) {
      return visit((Top) op);
    } else if (op instanceof Sort) {
      return visit((Sort) op);
    }
    throw new IllegalArgumentException("Unknown operator type: " + op);
  }

  @Override
  default void endVisit(PlanNode op) {
    if (op instanceof Project) {
      endVisit((Project) op);
    } else if (op instanceof Filter) {
      endVisit((Filter) op);
    } else if (op instanceof Join) {
      endVisit((Join) op);
    } else if (op instanceof Group) {
      endVisit((Group) op);
    } else if (op instanceof TableScan) {
      endVisit((TableScan) op);
    } else if (op instanceof Top) {
      endVisit((Top) op);
    } else if (op instanceof Sort) {
      endVisit((Sort) op);
    } else {
      throw new IllegalArgumentException("Unknown operator type: " + op);
    }
  }

  default boolean visit(Project project) {
    return true;
  }

  default void endVisit(Project project) {}

  default boolean visit(Filter filter) {
    return true;
  }

  default void endVisit(Filter filter) {}

  default boolean visit(Join join) {
    return true;
  }

  default void endVisit(Join join) {}

  default boolean visit(Group group) {
    return true;
  }

  default void endVisit(Group group) {}

  default boolean visit(TableScan scan) {
    return true;
  }

  default void endVisit(TableScan scan) {}

  default boolean visit(Top top) {
    return true;
  }

  default void endVisit(Top top) {}

  default boolean visit(Sort sort) {
    return true;
  }

  default void endVisit(Sort sort) {}
}
