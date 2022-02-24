package org.opensearch.sql.planner.logical;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Literal;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 * AD logical plan.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalAD extends LogicalPlan {
  private final Map<String, Literal> arguments;

  public LogicalAD(LogicalPlan child, Map<String, Literal> arguments) {
    super(Collections.singletonList(child));
    this.arguments = arguments;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAD(this, context);
  }
}
