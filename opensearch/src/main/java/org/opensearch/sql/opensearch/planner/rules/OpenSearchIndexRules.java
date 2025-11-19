/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

public class OpenSearchIndexRules {
  private static final ProjectIndexScanRule PROJECT_INDEX_SCAN =
      ProjectIndexScanRule.Config.DEFAULT.toRule();
  private static final FilterIndexScanRule FILTER_INDEX_SCAN =
      FilterIndexScanRule.Config.DEFAULT.toRule();
  private static final AggregateIndexScanRule AGGREGATE_INDEX_SCAN =
      AggregateIndexScanRule.Config.DEFAULT.toRule();
  private static final AggregateIndexScanRule COUNT_STAR_INDEX_SCAN =
      AggregateIndexScanRule.Config.COUNT_STAR.toRule();
  // TODO: No need this rule once https://github.com/opensearch-project/sql/issues/4403 is addressed
  private static final AggregateIndexScanRule BUCKET_NON_NULL_AGG_INDEX_SCAN =
      AggregateIndexScanRule.Config.BUCKET_NON_NULL_AGG.toRule();
  private static final LimitIndexScanRule LIMIT_INDEX_SCAN =
      LimitIndexScanRule.Config.DEFAULT.toRule();
  private static final SortIndexScanRule SORT_INDEX_SCAN =
      SortIndexScanRule.Config.DEFAULT.toRule();
  private static final DedupPushdownRule DEDUP_PUSH_DOWN =
      DedupPushdownRule.Config.DEFAULT.toRule();
  private static final SortProjectExprTransposeRule SORT_PROJECT_EXPR_TRANSPOSE =
      SortProjectExprTransposeRule.Config.DEFAULT.toRule();
  private static final ExpandCollationOnProjectExprRule EXPAND_COLLATION_ON_PROJECT_EXPR =
      ExpandCollationOnProjectExprRule.Config.DEFAULT.toRule();
  private static final SortAggregateMeasureRule SORT_AGGREGATION_METRICS_RULE =
      SortAggregateMeasureRule.Config.DEFAULT.toRule();
  private static final RareTopPushdownRule RARE_TOP_PUSH_DOWN =
      RareTopPushdownRule.Config.DEFAULT.toRule();

  // Rule that always pushes down relevance functions regardless of pushdown settings
  public static final RelevanceFunctionPushdownRule RELEVANCE_FUNCTION_PUSHDOWN =
      RelevanceFunctionPushdownRule.Config.DEFAULT.toRule();

  public static final List<RelOptRule> OPEN_SEARCH_INDEX_SCAN_RULES =
      ImmutableList.of(
          PROJECT_INDEX_SCAN,
          FILTER_INDEX_SCAN,
          AGGREGATE_INDEX_SCAN,
          COUNT_STAR_INDEX_SCAN,
          BUCKET_NON_NULL_AGG_INDEX_SCAN,
          LIMIT_INDEX_SCAN,
          SORT_INDEX_SCAN,
          // TODO enable if https://github.com/opensearch-project/OpenSearch/issues/3725 resolved
          // DEDUP_PUSH_DOWN,
          SORT_PROJECT_EXPR_TRANSPOSE,
          SORT_AGGREGATION_METRICS_RULE,
          RARE_TOP_PUSH_DOWN,
          EXPAND_COLLATION_ON_PROJECT_EXPR);

  // prevent instantiation
  private OpenSearchIndexRules() {}
}
