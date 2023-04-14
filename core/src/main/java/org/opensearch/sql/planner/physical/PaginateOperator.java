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
import org.opensearch.sql.planner.SerializablePlan;

@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class PaginateOperator extends PhysicalPlan implements SerializablePlan {
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
  private int pageIndex = 0;

  private int numReturned = 0;

  /**
   * Page given physical plan, with pageSize elements per page, starting with the given page.
   */
  public PaginateOperator(PhysicalPlan input, int pageSize, int pageIndex) {
    this.pageSize = pageSize;
    this.input = input;
    this.pageIndex = pageIndex;
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

  /** No need to serialize a PaginateOperator, it actually does nothing - it is a wrapper. */
  @Override
  public SerializablePlan getPlanForSerialization() {
    return (SerializablePlan) input;
  }
}
