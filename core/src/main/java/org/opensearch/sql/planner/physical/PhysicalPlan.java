/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.PlanNode;

/**
 * Physical plan.
 */
public abstract class PhysicalPlan implements PlanNode<PhysicalPlan>,
    Iterator<ExprValue>,
    AutoCloseable {
  /**
   * Accept the {@link PhysicalPlanNodeVisitor}.
   *
   * @param visitor visitor.
   * @param context visitor context.
   * @param <R>     returned object type.
   * @param <C>     context type.
   * @return returned object.
   */
  public abstract <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context);

  public void open() {
    getChild().forEach(PhysicalPlan::open);
  }

  public void close() {
    getChild().forEach(PhysicalPlan::close);
  }

  /**
   * schema.
   */
  public ExecutionEngine.Schema schema() {
    // throw new IllegalStateException(String.format("[BUG] schema can been only applied to "
    //       + "ProjectOperator, instead of %s", toString()));
    return new ExecutionEngine.Schema(ImmutableList.of());
  }
}
