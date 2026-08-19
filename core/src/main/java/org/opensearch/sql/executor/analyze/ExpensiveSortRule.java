/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;
import org.opensearch.sql.executor.AnalyzeResponse.RecommendationSeverityLevel;
import org.opensearch.sql.monitor.profile.QueryProfile.PlanNode;

/** Flags sorts over large inputs that consumed a large share of execution time. */
class ExpensiveSortRule implements RecommendationRule {

  /** Fires when the sort's self-time fraction of execute time exceeds this. */
  private static final double TIME_FRACTION = 0.20;

  /** Additionally requires at least this many input rows. */
  private static final long MIN_ROWS = 50_000;

  @Override
  public List<Recommendation> apply(ProfileView view) {
    List<Recommendation> recommendations = new ArrayList<>();
    double executeMs = view.phaseTime("execute");
    if (executeMs <= 0) {
      return recommendations;
    }
    for (PlanNode node : view.planNodes()) {
      if (!node.getNode().toLowerCase(Locale.ROOT).contains("sort")) {
        continue;
      }
      long rowsIn = ProfileView.rowsIn(node);
      double durationMs = ProfileView.duration(node);
      double timeFraction = durationMs / executeMs;
      if (timeFraction > TIME_FRACTION && rowsIn > MIN_ROWS) {
        long pct = Math.round(timeFraction * 100);
        recommendations.add(
            Recommendation.builder()
                .severity(RecommendationSeverityLevel.WARNING)
                .rule("Expensive Sort")
                .message(
                    "Sorting "
                        + rowsIn
                        + " rows took "
                        + durationMs
                        + " ms ("
                        + pct
                        + "% of execution)")
                .affected_node(node.getNode())
                .suggestion("Filter or limit rows before sorting (e.g. add head or a where).")
                .build());
      }
    }
    return recommendations;
  }
}
