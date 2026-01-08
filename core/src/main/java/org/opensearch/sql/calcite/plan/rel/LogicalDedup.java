/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import static org.opensearch.sql.calcite.plan.rule.PPLDedupConvertRule.DEDUP_CONVERT_RULE;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

public class LogicalDedup extends Dedup {

  protected LogicalDedup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    super(cluster, traitSet, input, dedupeFields, allowedDuplication, keepEmpty, consecutive);
  }

  @Override
  public Dedup copy(
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalDedup(
        getCluster(), traitSet, input, dedupeFields, allowedDuplication, keepEmpty, consecutive);
  }

  public static LogicalDedup create(
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    final RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalDedup(
        cluster, traitSet, input, dedupeFields, allowedDuplication, keepEmpty, consecutive);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(DEDUP_CONVERT_RULE);
  }
}
