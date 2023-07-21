/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;

/*
 * AD logical plan.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalAD extends LogicalPlan {
  private final Map<String, Literal> arguments;

  /**
   * Constructor of LogicalAD.
   * @param child child logical plan
   * @param arguments arguments of the algorithm
   */
  public LogicalAD(LogicalPlan child, Map<String, Literal> arguments) {
    super(Collections.singletonList(child));
    this.arguments = arguments;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAD(this, context);
  }
}
