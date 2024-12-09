/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.expression.Expression;

/** Logical Rare and TopN Plan. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalRareTopN extends LogicalPlan {

  private final CommandType commandType;
  private final Integer noOfResults;
  private final List<Expression> fieldList;
  private final List<Expression> groupByList;

  /** Constructor of LogicalRareTopN. */
  public LogicalRareTopN(
      LogicalPlan child,
      CommandType commandType,
      Integer noOfResults,
      List<Expression> fieldList,
      List<Expression> groupByList) {
    super(List.of(child));
    this.commandType = commandType;
    this.noOfResults = noOfResults;
    this.fieldList = fieldList;
    this.groupByList = groupByList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRareTopN(this, context);
  }
}
