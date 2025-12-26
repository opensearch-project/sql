/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The different between this and {@link EnumerableLimitSort} is the cost. */
public class CalciteEnumerableTopK extends EnumerableLimitSort {

  public CalciteEnumerableTopK(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost original = super.computeSelfCost(planner, mq);
    if (original == null) {
      return null;
    }
    // CalciteEnumerableTopK is converted by merging EnumerableLimit + EnumerableSort.
    // The cost should be less than cost(EnumerableLimit) + cost(CalciteEnumerableTopK)
    // which equals to getRows() * 2 * 0.99
    return planner
        .getCostFactory()
        .makeCost(original.getRows() * 2 * 0.99, original.getCpu(), original.getIo());
  }

  public static CalciteEnumerableTopK create(
      RelNode input, RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE).replace(collation);
    return new CalciteEnumerableTopK(cluster, traitSet, input, collation, offset, fetch);
  }
}
