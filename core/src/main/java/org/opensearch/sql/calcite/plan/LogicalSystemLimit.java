/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** System level limit logical plan, comparing to user level plan {@link LogicalSort}. */
public class LogicalSystemLimit extends Sort {

  private LogicalSystemLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    this(cluster, traitSet, Collections.emptyList(), input, collation, offset, fetch);
  }

  private LogicalSystemLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, hints, input, collation, offset, fetch);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  public static LogicalSystemLimit create(
      RelNode input, RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet = input.getTraitSet().replace(Convention.NONE).replace(collation);
    return new LogicalSystemLimit(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new LogicalSystemLimit(
        getCluster(), traitSet, hints, newInput, newCollation, offset, fetch);
  }
}
