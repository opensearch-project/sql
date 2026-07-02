/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

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
import org.opensearch.sql.calcite.plan.Scannable;

/** The physical relational operator representing a scan of a {@link RestSourceTable}. */
public class CalciteEnumerableRestScan extends AbstractCalciteRestScan
    implements EnumerableRel, Scannable {
  public CalciteEnumerableRestScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      RestSourceTable restTable,
      RelDataType schema) {
    super(
        cluster,
        cluster.traitSetOf(EnumerableConvention.INSTANCE),
        hints,
        table,
        restTable,
        schema);
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

    Expression scanOperator = implementor.stash(this, CalciteEnumerableRestScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  @Override
  public Enumerable<@Nullable Object> scan() {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new RestEnumerator(
            getFieldPath(),
            restTable.createRestRequest(),
            restTable.createOpenSearchResourceMonitor());
      }
    };
  }

  private List<String> getFieldPath() {
    return getRowType().getFieldNames().stream().toList();
  }
}
