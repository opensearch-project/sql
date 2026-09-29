/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;
import org.opensearch.sql.monitor.profile.QueryProfile;

/**
 * Builds the list of {@link Recommendation}s returned by the PPL {@code analyze} endpoint from the
 * {@link QueryProfile}'s plan-node tree and phase timings.
 *
 * <p>Each rule is a {@link RecommendationRule} strategy registered in {@link #RULES}. {@link
 * #build()} runs every registered rule against a shared {@link ProfileView} and concatenates the
 * results, so adding a rule is a one-line registry change plus a new rule class.
 */
public class AnalyzeRecommendationBuilder {

  /** Ordered registry of recommendation rules. */
  private static final List<RecommendationRule> RULES =
      List.of(
          new IneffectiveFilterRule(),
          new JoinRowExplosionRule(),
          new ExpensiveSortRule(),
          new BottleneckStageRule(),
          new OptimizePhaseDominatesRule());

  private final QueryProfile profile;

  public AnalyzeRecommendationBuilder(QueryProfile profile) {
    this.profile = profile;
  }

  /** Runs every registered rule and concatenates the ones that fired. */
  public List<Recommendation> build() {
    List<Recommendation> recommendations = new ArrayList<>();
    if (profile == null) {
      return recommendations;
    }
    ProfileView view = new ProfileView(profile);
    for (RecommendationRule rule : RULES) {
      recommendations.addAll(rule.apply(view));
    }
    return recommendations;
  }
}
