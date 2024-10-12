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
import org.opensearch.sql.ast.tree.Trendline;

/*
 * Trendline logical plan.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalTrendline extends LogicalPlan {
  private final List<Trendline.TrendlineComputation> computations;

  /**
   * Constructor of LogicalTrendline.
   *
   * @param child child logical plan
   * @param computations the computations for this trendline call.
   */
  public LogicalTrendline(LogicalPlan child, List<Trendline.TrendlineComputation> computations) {
    super(Collections.singletonList(child));
    this.computations = computations;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTrendline(this, context);
  }
}
