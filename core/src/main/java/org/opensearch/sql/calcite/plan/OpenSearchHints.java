/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import static org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_AGGREGATE_RULE;

import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class OpenSearchHints {

  public static final HintStrategy aggStatsArgs =
      HintStrategy.builder((hint, rel) -> hint.hintName.equals("stats_args") && rel instanceof LogicalAggregate)
      .excludedRules(ENUMERABLE_AGGREGATE_RULE)
      .converterRules(OpenSearchRules.AGGREGATE_HINT_CONVERT_RULE)
      .build();

  // prevent instantiation
  private OpenSearchHints() {}
}
