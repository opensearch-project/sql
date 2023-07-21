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

/**
 * ml-commons logical plan.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalMLCommons extends LogicalPlan {
  private final String algorithm;

  private final  Map<String, Literal> arguments;

  /**
   * Constructor of LogicalMLCommons.
   * @param child child logical plan
   * @param algorithm algorithm name
   * @param arguments arguments of the algorithm
   */
  public LogicalMLCommons(LogicalPlan child, String algorithm,
                          Map<String, Literal> arguments) {
    super(Collections.singletonList(child));
    this.algorithm = algorithm;
    this.arguments = arguments;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitMLCommons(this, context);
  }
}
