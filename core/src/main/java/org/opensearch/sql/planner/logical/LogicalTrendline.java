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
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.data.type.ExprCoreType;

/*
 * Trendline logical plan.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalTrendline extends LogicalPlan {
  private final List<Pair<Trendline.TrendlineComputation, ExprCoreType>> computations;

  /**
   * Constructor of LogicalTrendline.
   *
   * @param child child logical plan
   * @param computations the computations for this trendline call.
   */
  public LogicalTrendline(
      LogicalPlan child, List<Pair<Trendline.TrendlineComputation, ExprCoreType>> computations) {
    super(Collections.singletonList(child));
    this.computations = computations;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTrendline(this, context);
  }
}
