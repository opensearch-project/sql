/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import java.util.Collections;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * A {@code TableScanBuilder} represents transition state between logical planning
 * and physical planning.
 */
public abstract class TableScanBuilder extends LogicalPlan {

  public TableScanBuilder() {
    super(Collections.emptyList());
  }

  /**
   * Build table scan operator.
   * @return table scan operator
   */
  public abstract TableScanOperator build();

  /**
   * Can a given filter be pushed down to table scan builder. Assume no such support
   * by default unless subclass override this.
   *
   * @param filter filter node
   * @return true if pushed down, otherwise false
   */
  public boolean pushDownFilter(LogicalFilter filter) {
    return false;
  }

  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    return false;
  }

  public boolean pushDownSort(LogicalSort sort) {
    return false;
  }

  public boolean pushDownLimit(LogicalLimit limit) {
    return false;
  }

  public boolean pushDownProject(LogicalProject project) {
    return false;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitScanBuilder(this, context);
  }
}
