/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.CoreRules;

/** Relational expression representing a scan of an OpenSearch type. */
public abstract class OpenSearchTableScan extends TableScan implements EnumerableRel {
  /**
   * Creates an OpenSearchTableScan.
   *
   * @param cluster Cluster
   * @param table Table
   */
  protected OpenSearchTableScan(RelOptCluster cluster, RelOptTable table) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : OpenSearchRules.OPEN_SEARCH_OPT_RULES) {
      planner.addRule(rule);
    }

    // remove this rule otherwise opensearch can't correctly interpret approx_count_distinct()
    // it is converted to cardinality aggregation in OpenSearch
    planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
  }
}
