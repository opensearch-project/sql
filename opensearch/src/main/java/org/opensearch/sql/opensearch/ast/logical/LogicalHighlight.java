/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.logical;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.logical.LogicalPlan;

@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalHighlight extends OpenSearchLogicalPlan {
  private final Expression highlightField;
  private final Map<String, Literal> arguments;

  /** Constructor of LogicalHighlight. */
  public LogicalHighlight(
      LogicalPlan childPlan, Expression highlightField, Map<String, Literal> arguments) {
    super(Collections.singletonList(childPlan));
    this.highlightField = highlightField;
    this.arguments = arguments;
  }

  @Override
  public <R, C> R accept(OpenSearchLogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }
}
