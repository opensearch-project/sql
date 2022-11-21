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
 * A {@code TableScanBuilder} represents transition state between logical planning and
 * physical planning. The concrete implementation class gets involved in the logical
 * optimization through this abstraction and thus get the chance to handle push down
 * optimization without intruding core engine.
 */
public abstract class TableScanBuilder extends LogicalPlan {

  /**
   * Construct and initialize children to empty list.
   */
  public TableScanBuilder() {
    super(Collections.emptyList());
  }

  /**
   * Build table scan operator.
   *
   * @return table scan operator
   */
  public abstract TableScanOperator build();

  /**
   * Can a given filter operator be pushed down to table scan builder. Assume no such support
   * by default unless subclass override this.
   *
   * @param filter logical filter operator
   * @return true if pushed down, otherwise false
   */
  public boolean pushDownFilter(LogicalFilter filter) {
    return false;
  }

  /**
   * Can a given aggregate operator be pushed down to table scan builder. Assume no such support
   * by default unless subclass override this.
   *
   * @param aggregation logical aggregate operator
   * @return true if pushed down, otherwise false
   */
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    return false;
  }

  /**
   * Can a given sort operator be pushed down to table scan builder. Assume no such support
   * by default unless subclass override this.
   *
   * @param sort logical sort operator
   * @return true if pushed down, otherwise false
   */
  public boolean pushDownSort(LogicalSort sort) {
    return false;
  }

  /**
   * Can a given limit operator be pushed down to table scan builder. Assume no such support
   * by default unless subclass override this.
   *
   * @param limit logical limit operator
   * @return true if pushed down, otherwise false
   */
  public boolean pushDownLimit(LogicalLimit limit) {
    return false;
  }

  /**
   * Can a given project operator be pushed down to table scan builder. Assume no such support
   * by default unless subclass override this.
   *
   * @param project logical project operator
   * @return true if pushed down, otherwise false
   */
  public boolean pushDownProject(LogicalProject project) {
    return false;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitScanBuilder(this, context);
  }
}
