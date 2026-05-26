/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

public class OpenSearchIndexRules {

  private static final RelOptRule INDEX_SCAN_RULE = EnumerableIndexScanRule.DEFAULT_CONFIG.toRule();
  private static final RelOptRule SYSTEM_INDEX_SCAN_RULE =
      EnumerableSystemIndexScanRule.DEFAULT_CONFIG.toRule();
  private static final RelOptRule NESTED_AGGREGATE_RULE =
      EnumerableNestedAggregateRule.DEFAULT_CONFIG.toRule();
  private static final RelOptRule GRAPH_LOOKUP_RULE =
      EnumerableGraphLookupRule.DEFAULT_CONFIG.toRule();
  // Rule that always pushes down relevance functions regardless of pushdown settings
  private static final RelevanceFunctionPushdownRule RELEVANCE_FUNCTION_RULE =
      RelevanceFunctionPushdownRule.Config.DEFAULT.toRule();

  /** The rules will apply whatever the pushdown setting is. */
  public static final List<RelOptRule> OPEN_SEARCH_NON_PUSHDOWN_RULES =
      ImmutableList.of(
          INDEX_SCAN_RULE,
          SYSTEM_INDEX_SCAN_RULE,
          NESTED_AGGREGATE_RULE,
          GRAPH_LOOKUP_RULE,
          RELEVANCE_FUNCTION_RULE);

  private static final ProjectIndexScanRule PROJECT_INDEX_SCAN =
      ProjectIndexScanRule.Config.DEFAULT.toRule();
  private static final FilterIndexScanRule FILTER_INDEX_SCAN =
      FilterIndexScanRule.Config.DEFAULT.toRule();
  private static final AggregateIndexScanRule AGGREGATE_PROJECT_INDEX_SCAN =
      AggregateIndexScanRule.Config.DEFAULT.toRule();
  private static final AggregateIndexScanRule AGGREGATE_INDEX_SCAN =
      AggregateIndexScanRule.Config.AGGREGATE_SCAN.toRule();
  // TODO: No need this rule once https://github.com/opensearch-project/sql/issues/4403 is addressed
  private static final AggregateIndexScanRule BUCKET_NON_NULL_AGG_INDEX_SCAN =
      AggregateIndexScanRule.Config.BUCKET_NON_NULL_AGG.toRule();
  private static final AggregateIndexScanRule BUCKET_NON_NULL_AGG_WITH_UDF_INDEX_SCAN =
      AggregateIndexScanRule.Config.BUCKET_NON_NULL_AGG_WITH_UDF.toRule();
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
  private static final SortExprIndexScanRule SORT_EXPR_INDEX_SCAN =
      SortExprIndexScanRule.Config.DEFAULT.toRule();
  private static final EnumerableTopKMergeRule ENUMERABLE_TOP_K_MERGE_RULE =
      EnumerableTopKMergeRule.Config.DEFAULT.toRule();

  /** The rules will apply only when the pushdown is enabled. */
  public static final List<RelOptRule> OPEN_SEARCH_PUSHDOWN_RULES =
      ImmutableList.of(
          PROJECT_INDEX_SCAN,
          FILTER_INDEX_SCAN,
          AGGREGATE_PROJECT_INDEX_SCAN,
          AGGREGATE_INDEX_SCAN,
          BUCKET_NON_NULL_AGG_INDEX_SCAN,
          BUCKET_NON_NULL_AGG_WITH_UDF_INDEX_SCAN,
          LIMIT_INDEX_SCAN,
          SORT_INDEX_SCAN,
          DEDUP_PUSH_DOWN,
          SORT_PROJECT_EXPR_TRANSPOSE,
          SORT_AGGREGATION_METRICS_RULE,
          RARE_TOP_PUSH_DOWN,
          ENUMERABLE_TOP_K_MERGE_RULE,
          EXPAND_COLLATION_ON_PROJECT_EXPR,
          SORT_EXPR_INDEX_SCAN);

  // prevent instantiation
  private OpenSearchIndexRules() {}
}
