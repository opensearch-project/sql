/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.List;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The enumerable aggregate physical implementation for OpenSearch nested aggregation.
 * https://docs.opensearch.org/latest/aggregations/bucket/nested/
 */
public class CalciteEnumerableNestedAggregate extends EnumerableAggregateBase
    implements EnumerableRel {

  public CalciteEnumerableNestedAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    assert getConvention() instanceof EnumerableConvention;

    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException("distinct aggregation not supported");
      }
      if (aggCall.distinctKeys != null) {
        throw new InvalidRelException("within-distinct aggregation not supported");
      }
      AggImplementor implementor2 = RexImpTable.INSTANCE.get(aggCall.getAggregation(), false);
      if (implementor2 == null) {
        throw new InvalidRelException("aggregation " + aggCall.getAggregation() + " not supported");
      }
    }
  }

  @Override
  public CalciteEnumerableNestedAggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    try {
      return new CalciteEnumerableNestedAggregate(
          getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // TODO implement an enumerable nested aggregate
    throw new UnsupportedOperationException(
        String.format(
            "Cannot execute nested aggregation on %s since pushdown cannot be applied.", aggCalls));
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.9);
  }
}
