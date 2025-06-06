/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

public class OpenSearchIndexRules {
  private static final OpenSearchProjectIndexScanRule PROJECT_INDEX_SCAN =
      OpenSearchProjectIndexScanRule.Config.DEFAULT.toRule();
  private static final OpenSearchFilterIndexScanRule FILTER_INDEX_SCAN =
      OpenSearchFilterIndexScanRule.Config.DEFAULT.toRule();
  private static final OpenSearchAggregateIndexScanRule AGGREGATE_INDEX_SCAN =
      OpenSearchAggregateIndexScanRule.Config.DEFAULT.toRule();
  private static final OpenSearchLimitIndexScanRule LIMIT_INDEX_SCAN =
      OpenSearchLimitIndexScanRule.Config.DEFAULT.toRule();

  public static final List<RelOptRule> OPEN_SEARCH_INDEX_SCAN_RULES =
      ImmutableList.of(
          PROJECT_INDEX_SCAN, FILTER_INDEX_SCAN, AGGREGATE_INDEX_SCAN, LIMIT_INDEX_SCAN);

  // prevent instantiation
  private OpenSearchIndexRules() {}
}
