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

/** Flags filter stages that barely reduced their input. */
class IneffectiveFilterRule implements RecommendationRule {

  /** Fires when rows_out / rows_in exceeds this pass-through ratio. */
  private static final double MAX_PASS_RATIO = 0.95;

  @Override
  public List<Recommendation> apply(ProfileView view) {
    List<Recommendation> recommendations = new ArrayList<>();
    for (PlanNode node : view.planNodes()) {
      String name = node.getNode().toLowerCase(Locale.ROOT);
      if (!name.contains("filter")) {
        continue;
      }
      long rowsIn = ProfileView.rowsIn(node);
      if (rowsIn <= 0) {
        continue;
      }
      double ratio = (double) node.getRows() / rowsIn;
      if (ratio > MAX_PASS_RATIO) {
        long droppedPct = Math.round((1.0 - ratio) * 100);
        recommendations.add(
            Recommendation.builder()
                .severity(RecommendationSeverityLevel.WARNING)
                .rule("Ineffective Filter")
                .message("Filter only dropped " + droppedPct + "% of rows")
                .affected_node(node.getNode())
                .suggestion("Consider removing the filter or making it more selective.")
                .build());
      }
    }
    return recommendations;
  }
}
