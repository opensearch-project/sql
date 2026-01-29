/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

public class OpenSearchRules {
  public static final PPLAggregateConvertRule AGGREGATE_CONVERT_RULE =
      PPLAggregateConvertRule.Config.SUM_CONVERTER.toRule();
  public static final PPLAggGroupMergeRule AGG_GROUP_MERGE_RULE =
      PPLAggGroupMergeRule.Config.GROUP_MERGE.toRule();

  public static final List<RelOptRule> OPEN_SEARCH_OPT_RULES =
      ImmutableList.of(AGGREGATE_CONVERT_RULE, AGG_GROUP_MERGE_RULE);

  // prevent instantiation
  private OpenSearchRules() {}
}
