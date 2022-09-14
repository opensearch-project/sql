package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.storage.Table;

/**
 * Logical Table Functions...Function which outputs table.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalTableFunction extends LogicalPlan {

  @Getter
  private final Expression tableFunction;

  @Getter
  private final Table table;

  public LogicalTableFunction(Expression tableFunction, Table table) {
    super(ImmutableList.of());
    this.tableFunction = tableFunction;
    this.table = table;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTableFunction(this, context);
  }

}
