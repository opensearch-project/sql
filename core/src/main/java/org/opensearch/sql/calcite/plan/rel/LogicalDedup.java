/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import static org.opensearch.sql.calcite.plan.rule.PPLDedupConvertRule.DEDUP_CONVERT_RULE;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
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
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames) {
    super(
        cluster,
        traitSet,
        input,
        dedupeFields,
        allowedDuplication,
        keepEmpty,
        consecutive,
        inputCollation,
        inputCollationFieldNames);
  }

  @Override
  public Dedup copy(
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalDedup(
        getCluster(),
        traitSet,
        input,
        dedupeFields,
        allowedDuplication,
        keepEmpty,
        consecutive,
        inputCollation,
        inputCollationFieldNames);
  }

  public static LogicalDedup create(
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    return create(input, dedupeFields, allowedDuplication, keepEmpty, consecutive, null);
  }

  public static LogicalDedup create(
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation) {
    // Capture field names from the current input row type at construction time — the collation's
    // RexInputRef indices refer to this row type and may not be resolvable later if planner rules
    // narrow the dedup's input.
    List<String> inputCollationFieldNames =
        inputCollation == null ? null : input.getRowType().getFieldNames();
    return create(
        input,
        dedupeFields,
        allowedDuplication,
        keepEmpty,
        consecutive,
        inputCollation,
        inputCollationFieldNames);
  }

  public static LogicalDedup create(
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames) {
    final RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalDedup(
        cluster,
        traitSet,
        input,
        dedupeFields,
        allowedDuplication,
        keepEmpty,
        consecutive,
        inputCollation,
        inputCollationFieldNames);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(DEDUP_CONVERT_RULE);
  }
}
