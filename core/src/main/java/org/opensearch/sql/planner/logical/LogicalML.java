package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;

/**
 * ML logical plan.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalML extends LogicalPlan {
  private final  Map<String, Literal> arguments;

  /**
   * Constructor of LogicalML.
   * @param child child logical plan
   * @param arguments arguments of the algorithm
   */
  public LogicalML(LogicalPlan child, Map<String, Literal> arguments) {
    super(Collections.singletonList(child));
    this.arguments = arguments;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitML(this, context);
  }
}
