/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
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
  final @Nullable RelCollation inputCollation;

  /**
   * Field names of the input collation, captured at construction time against the row type that
   * produced the collation. Kept separately because later planner rules can narrow this dedup's
   * input row type, making the index-based {@link RelCollation} unsafe to resolve via {@code
   * getInput().getRowType()} alone. {@code null} iff {@link #inputCollation} is {@code null}.
   */
  final @Nullable List<String> inputCollationFieldNames;

  protected Dedup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames) {
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
    this.inputCollation = inputCollation;
    this.inputCollationFieldNames = inputCollationFieldNames;
  }

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(
        traitSet,
        sole(inputs),
        this.dedupeFields,
        this.allowedDuplication,
        this.keepEmpty,
        this.consecutive,
        this.inputCollation,
        this.inputCollationFieldNames);
  }

  public abstract Dedup copy(
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames);

  public Dedup copy(RelNode input, List<RexNode> dedupeFields) {
    return this.copy(
        this.getTraitSet(),
        input,
        dedupeFields,
        this.allowedDuplication,
        this.keepEmpty,
        this.consecutive,
        this.inputCollation,
        this.inputCollationFieldNames);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("dedup_fields", dedupeFields)
        .item("allowed_dedup", allowedDuplication)
        .item("keepEmpty", keepEmpty)
        .item("consecutive", consecutive)
        .itemIf("inputCollation", inputCollation, inputCollation != null);
  }

  @Override
  public void register(RelOptPlanner planner) {}
}
