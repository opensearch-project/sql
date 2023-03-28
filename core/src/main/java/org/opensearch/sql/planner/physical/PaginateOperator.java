/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;

@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class PaginateOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;

  @Getter
  private final int pageSize;

  /**
   * Which page is this?
   * May not be necessary in the end. Currently used to increment the "cursor counter" --
   * See usage.
   */
  @Getter
  private final int pageIndex;

  int numReturned = 0;

  /**
   * Page given physical plan, with pageSize elements per page, starting with the first page.
   */
  public PaginateOperator(PhysicalPlan input, int pageSize) {
    this.pageSize = pageSize;
    this.input = input;
    this.pageIndex = 0;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitPaginate(this, context);
  }

  @Override
  public boolean hasNext() {
    return numReturned < pageSize && input.hasNext();
  }

  @Override
  public ExprValue next() {
    numReturned += 1;
    return input.next();
  }

  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return input.schema();
  }

  @Override
  public String toCursor() {
    // Save cursor to read the next page.
    // Could process node.getChild() here with another visitor -- one that saves the
    // parameters for other physical operators -- ProjectOperator, etc.
    // cursor format: n:<paginate(next-page, pagesize)>|<child>"
    String child = getChild().get(0).toCursor();

    var nextPage = getPageIndex() + 1;
    return child == null || child.isEmpty()
        ? null : createSection("Paginate", Integer.toString(nextPage),
            Integer.toString(getPageSize()), child);
  }
}
