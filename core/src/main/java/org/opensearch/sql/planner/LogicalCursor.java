package org.opensearch.sql.planner;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.storage.StorageEngine;

@EqualsAndHashCode(callSuper = false)
@ToString
public class LogicalCursor extends LogicalPlan {
  @Getter
  private final String cursor;

  @Getter
  private final StorageEngine engine;

  public LogicalCursor(String cursor, StorageEngine engine) {
    super(List.of());
    this.cursor = cursor;
    this.engine = engine;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCursor(this, context);
  }
}