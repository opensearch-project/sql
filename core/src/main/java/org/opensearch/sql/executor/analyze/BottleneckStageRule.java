/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.List;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;
import org.opensearch.sql.executor.AnalyzeResponse.RecommendationSeverityLevel;
import org.opensearch.sql.monitor.profile.QueryProfile.PlanNode;

/** Flags the single stage that dominated execution time. */
class BottleneckStageRule implements RecommendationRule {

  /** Fires when the slowest node's self-time fraction of execute exceeds this. */
  private static final double TIME_FRACTION = 0.75;

  @Override
  public List<Recommendation> apply(ProfileView view) {
    double executeMs = view.phaseTime("execute");
    if (executeMs <= 0) {
      return List.of();
    }
    PlanNode slowest = null;
    double slowestDuration = 0;
    for (PlanNode node : view.planNodes()) {
      double durationMs = ProfileView.duration(node);
      if (slowest == null || durationMs > slowestDuration) {
        slowest = node;
        slowestDuration = durationMs;
      }
    }
    if (slowest == null) {
      return List.of();
    }
    double timeFraction = slowestDuration / executeMs;
    if (timeFraction <= TIME_FRACTION) {
      return List.of();
    }
    long pct = Math.round(timeFraction * 100);
    return List.of(
        Recommendation.builder()
            .severity(RecommendationSeverityLevel.INFO)
            .rule("Bottleneck Stage")
            .message(
                slowest.getNode() + " took " + slowestDuration + " ms (" + pct + "% of execution)")
            .affected_node(slowest.getNode())
            .build());
  }
}
