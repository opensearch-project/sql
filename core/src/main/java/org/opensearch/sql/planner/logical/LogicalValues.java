/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.LiteralExpression;

/**
 * Logical operator which is a sequence of literal rows (like a relation).<br>
 * Basically, Values operator is used to create rows of constant literals<br>
 * "out of nothing" which is corresponding with VALUES clause in SQL. <br>
 * Mostly all rows must have the same number of literals and each column should have same type or
 * can be converted implicitly. In particular, typical use cases include:
 *
 * <ol>
 *   <li>Project without relation involved.
 *   <li>Defining query or insertion without a relation.
 * </ol>
 *
 * Take the following logical plan for example:
 *
 * <p>LogicalProject(expr=[log(2),true,1+2])<br>
 * &ensp; |_ LogicalValues([[]]) #an empty row so that Project can evaluate its expressions in
 * next()
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = true)
public class LogicalValues extends LogicalPlan {

  private final List<List<LiteralExpression>> values;

  /** Constructor of LogicalValues. */
  public LogicalValues(List<List<LiteralExpression>> values) {
    super(ImmutableList.of());
    this.values = values;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitValues(this, context);
  }
}
