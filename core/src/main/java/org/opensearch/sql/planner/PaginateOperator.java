/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;

@RequiredArgsConstructor
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
  public void open() {
    super.open();
    numReturned = 0;
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
    assert input instanceof ProjectOperator;
    return input.schema();
  }
}
