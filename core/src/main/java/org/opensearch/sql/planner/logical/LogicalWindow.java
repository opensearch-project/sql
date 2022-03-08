/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.window.WindowDefinition;

/**
 * Logical operator for window function generated from project list. Logically, each window operator
 * has to work with a Sort operator to ensure input data is sorted as required by window definition.
 * However, the Sort operator may be removed after logical optimization.
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalWindow extends LogicalPlan {
  private final NamedExpression windowFunction;
  private final WindowDefinition windowDefinition;

  /**
   * Constructor of logical window.
   */
  public LogicalWindow(
      LogicalPlan child,
      NamedExpression windowFunction,
      WindowDefinition windowDefinition) {
    super(Collections.singletonList(child));
    this.windowFunction = windowFunction;
    this.windowDefinition = windowDefinition;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }

}
