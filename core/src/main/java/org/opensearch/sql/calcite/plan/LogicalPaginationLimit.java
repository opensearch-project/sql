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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pagination limit logical plan for API-level pagination (offset/pageSize). This is applied AFTER
 * the user's query executes completely, including any user-specified head/head from commands.
 *
 * <p>Unlike regular LogicalSort which may be merged with other sorts by Calcite optimizer, this
 * class ensures pagination is applied as a final post-processing step on the query result.
 */
public class LogicalPaginationLimit extends Sort {

  private LogicalPaginationLimit(
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

  /**
   * Create a pagination limit node.
   *
   * @param input the input RelNode (user's complete query)
   * @param offset pagination offset (0-based)
   * @param fetch page size
   * @return LogicalPaginationLimit node
   */
  public static LogicalPaginationLimit create(
      RelNode input, @Nullable RexNode offset, @Nullable RexNode fetch) {
    RelOptCluster cluster = input.getCluster();
    // Preserve input's collation to ensure sort order is maintained
    List<RelCollation> collations = input.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    RelCollation collation = collations == null || collations.isEmpty() ? null : collations.get(0);
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet = input.getTraitSet().replace(Convention.NONE).replace(collation);
    return new LogicalPaginationLimit(
        cluster, traitSet, Collections.emptyList(), input, collation, offset, fetch);
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new LogicalPaginationLimit(
        getCluster(), traitSet, hints, newInput, newCollation, offset, fetch);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    // Mark this as pagination in explain output
    pw.item("type", "PAGINATION");
    return pw;
  }
}
