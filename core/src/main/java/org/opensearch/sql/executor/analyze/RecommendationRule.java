/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.List;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;

/**
 * A single analyze recommendation rule. Given a read-only view over the query profile, a rule emits
 * zero or more {@link Recommendation}s. Per-node rules may emit one per matching plan node;
 * whole-query rules emit at most one. Rules are stateless and registered in {@link
 * AnalyzeRecommendationBuilder}.
 */
interface RecommendationRule {

  /** Evaluate this rule against the profile and return any recommendations it produces. */
  List<Recommendation> apply(ProfileView view);
}
