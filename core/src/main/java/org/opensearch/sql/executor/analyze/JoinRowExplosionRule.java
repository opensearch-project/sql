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

/** Flags joins whose output greatly exceeds their combined input. */
class JoinRowExplosionRule implements RecommendationRule {

  /** Fires (WARNING) when rows_out / rows_in exceeds this ratio. */
  private static final double RATIO = 5.0;

  /**
   * Escalates to CRITICAL at or above this ratio. Not specified by the rule table; chosen as a
   * higher tier above {@link #RATIO}.
   */
  private static final double CRITICAL_RATIO = 20.0;

  @Override
  public List<Recommendation> apply(ProfileView view) {
    List<Recommendation> recommendations = new ArrayList<>();
    for (PlanNode node : view.planNodes()) {
      if (!node.getNode().toLowerCase(Locale.ROOT).contains("join")) {
        continue;
      }
      long rowsIn = ProfileView.rowsIn(node);
      if (rowsIn <= 0) {
        continue;
      }
      double ratio = (double) node.getRows() / rowsIn;
      if (ratio > RATIO) {
        RecommendationSeverityLevel severity =
            ratio >= CRITICAL_RATIO
                ? RecommendationSeverityLevel.CRITICAL
                : RecommendationSeverityLevel.WARNING;
        recommendations.add(
            Recommendation.builder()
                .severity(severity)
                .rule("Join Row Explosion")
                .message(
                    "Join expanded "
                        + rowsIn
                        + " rows into "
                        + node.getRows()
                        + " rows ("
                        + String.format(Locale.ROOT, "%.1f", ratio)
                        + "×)")
                .affected_node(node.getNode())
                .suggestion("Add filters to the subqueries before the join to reduce rows.")
                .build());
      }
    }
    return recommendations;
  }
}
