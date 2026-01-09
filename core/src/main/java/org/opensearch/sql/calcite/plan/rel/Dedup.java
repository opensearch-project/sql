/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.exception.CalciteUnsupportedException;

/** Relational expression representing a dedup command. */
@Getter
public abstract class Dedup extends SingleRel {
  final List<RexNode> dedupeFields;
  final Integer allowedDuplication;
  final Boolean keepEmpty;
  final Boolean consecutive;

  /** */
  protected Dedup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    super(cluster, traitSet, input);
    if (allowedDuplication <= 0) {
      throw new IllegalArgumentException("Number of duplicate events must be greater than 0");
    }
    if (consecutive) {
      throw new CalciteUnsupportedException("Consecutive deduplication is unsupported in Calcite");
    }
    this.dedupeFields = dedupeFields;
    this.allowedDuplication = allowedDuplication;
    this.keepEmpty = keepEmpty;
    this.consecutive = consecutive;
  }

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(
        traitSet,
        sole(inputs),
        this.dedupeFields,
        this.allowedDuplication,
        this.keepEmpty,
        this.consecutive);
  }

  public abstract Dedup copy(
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive);

  public Dedup copy(RelNode input, List<RexNode> dedupeFields) {
    return this.copy(
        this.getTraitSet(),
        input,
        dedupeFields,
        this.allowedDuplication,
        this.keepEmpty,
        this.consecutive);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("dedup_fields", dedupeFields)
        .item("allowed_dedup", allowedDuplication)
        .item("keepEmpty", keepEmpty)
        .item("consecutive", consecutive);
  }

  @Override
  public void register(RelOptPlanner planner) {}
}
