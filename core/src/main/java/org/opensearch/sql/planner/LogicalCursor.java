package org.opensearch.sql.planner;

import java.util.List;
import lombok.Getter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

public class LogicalCursor extends LogicalPlan {
  @Getter
  final String cursor;

  public LogicalCursor(String cursor) {
    super(List.of());
    this.cursor = cursor;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCursor(this, context);
  }
}
