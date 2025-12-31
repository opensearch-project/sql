/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Logical Convert represents the convert operation.
 *
 * <p>The {@link LogicalConvert#conversions} is a list of conversion operations where each Pair
 * represents (target_field, conversion_expression).
 *
 * <p>Example: convert auto(age), num(price) AS numeric_price translates to:
 *
 * <ul>
 *   <li>Pair(age, auto(age))
 *   <li>Pair(numeric_price, num(price))
 * </ul>
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalConvert extends LogicalPlan {

  @Getter private final List<Pair<ReferenceExpression, Expression>> conversions;

  @Getter private final String timeformat;

  /** Constructor of LogicalConvert. */
  public LogicalConvert(
      LogicalPlan child,
      List<Pair<ReferenceExpression, Expression>> conversions,
      String timeformat) {
    super(Collections.singletonList(child));
    this.conversions = conversions;
    this.timeformat = timeformat;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitConvert(this, context);
  }
}
