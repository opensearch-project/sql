package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;

@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalHighlight extends LogicalPlan {
  private final Expression highlightField;

  public LogicalHighlight(LogicalPlan childPlan, Expression field) {
    super(Collections.singletonList(childPlan));
    highlightField = field;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }
}
