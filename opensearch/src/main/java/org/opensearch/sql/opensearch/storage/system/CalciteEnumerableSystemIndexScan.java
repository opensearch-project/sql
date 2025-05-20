/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The physical relational operator representing a scan of an OpenSearchSystemIndex type. */
public class CalciteEnumerableSystemIndexScan extends AbstractCalciteSystemIndexScan
    implements EnumerableRel {
  public CalciteEnumerableSystemIndexScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchSystemIndex sysIndex,
      RelDataType schema) {
    super(
        cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), hints, table, sysIndex, schema);
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return EnumerableRel.super.passThroughTraits(required);
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      RelTraitSet childTraits, int childId) {
    return EnumerableRel.super.deriveTraits(childTraits, childId);
  }

  @Override
  public DeriveMode getDeriveMode() {
    return EnumerableRel.super.getDeriveMode();
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

    Expression scanOperator = implementor.stash(this, CalciteEnumerableSystemIndexScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  public Enumerable<@Nullable Object> scan() {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new OpenSearchSystemIndexEnumerator(
            getFieldPath(), sysIndex.getSystemIndexBundle().getRight());
      }
    };
  }

  private List<String> getFieldPath() {
    return getRowType().getFieldNames().stream().toList();
  }
}
