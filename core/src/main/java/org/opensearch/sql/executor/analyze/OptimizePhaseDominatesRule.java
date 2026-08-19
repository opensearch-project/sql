/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.List;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;
import org.opensearch.sql.executor.AnalyzeResponse.RecommendationSeverityLevel;

/** Flags queries that spent more time planning than executing. */
class OptimizePhaseDominatesRule implements RecommendationRule {

  /** Fires when optimize time exceeds this (ms) and beats execute time. */
  private static final double MIN_MS = 75.0;

  @Override
  public List<Recommendation> apply(ProfileView view) {
    double executeMs = view.phaseTime("execute");
    double optimizeMs = view.phaseTime("optimize");
    if (executeMs < optimizeMs && optimizeMs > MIN_MS) {
      return List.of(
          Recommendation.builder()
              .severity(RecommendationSeverityLevel.INFO)
              .rule("Optimize Phase Dominates")
              .message(
                  "Query planning took " + optimizeMs + " ms vs " + executeMs + " ms executing")
              .build());
    }
    return List.of();
  }
}
