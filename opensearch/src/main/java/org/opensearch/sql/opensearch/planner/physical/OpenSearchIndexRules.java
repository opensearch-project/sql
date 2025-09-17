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
  private static final OpenSearchAggregateIndexScanRule COUNT_STAR_INDEX_SCAN =
      OpenSearchAggregateIndexScanRule.Config.COUNT_STAR.toRule();
  private static final OpenSearchLimitIndexScanRule LIMIT_INDEX_SCAN =
      OpenSearchLimitIndexScanRule.Config.DEFAULT.toRule();
  private static final OpenSearchSortIndexScanRule SORT_INDEX_SCAN =
      OpenSearchSortIndexScanRule.Config.DEFAULT.toRule();
  private static final OpenSearchDedupPushdownRule DEDUP_PUSH_DOWN =
      OpenSearchDedupPushdownRule.Config.DEFAULT.toRule();
  private static final SortProjectExprTransposeRule SORT_PROJECT_EXPR_TRANSPOSE =
      SortProjectExprTransposeRule.Config.DEFAULT.toRule();
  private static final ExpandCollationOnProjectExprRule EXPAND_COLLATION_ON_PROJECT_EXPR =
      ExpandCollationOnProjectExprRule.Config.DEFAULT.toRule();

  // Rule that always pushes down relevance functions regardless of pushdown settings
  public static final OpenSearchRelevanceFunctionPushdownRule RELEVANCE_FUNCTION_PUSHDOWN =
      OpenSearchRelevanceFunctionPushdownRule.Config.DEFAULT.toRule();

  public static final List<RelOptRule> OPEN_SEARCH_INDEX_SCAN_RULES =
      ImmutableList.of(
          PROJECT_INDEX_SCAN,
          FILTER_INDEX_SCAN,
          AGGREGATE_INDEX_SCAN,
          COUNT_STAR_INDEX_SCAN,
          LIMIT_INDEX_SCAN,
          // TODO enable if https://github.com/opensearch-project/OpenSearch/issues/3725 resolved
          // DEDUP_PUSH_DOWN,
          SORT_INDEX_SCAN,
          SORT_PROJECT_EXPR_TRANSPOSE,
          EXPAND_COLLATION_ON_PROJECT_EXPR);

  // prevent instantiation
  private OpenSearchIndexRules() {}
}
