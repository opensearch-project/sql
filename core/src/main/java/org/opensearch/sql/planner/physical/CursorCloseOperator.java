/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * A plan node which blocks issuing a request in {@link #open} and
 * getting results in {@link #hasNext}, but doesn't block releasing resources in {@link #close}.
 * Designed to be on top of the deserialized tree.
 */
@RequiredArgsConstructor
public class CursorCloseOperator extends PhysicalPlan {

  // Entire deserialized from cursor plan tree
  private final PhysicalPlan input;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCursorClose(this, context);
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    throw new IllegalStateException();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  /**
   * Provides an empty schema, because this plan node is always located on the top of the tree.
   */
  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(List.of());
  }

  @Override
  public void open() {
    // no-op, no search should be invoked.
  }
}
