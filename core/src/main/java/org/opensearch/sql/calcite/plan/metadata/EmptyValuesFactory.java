/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.metadata;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/**
 * {@link RelFactories.ValuesFactory} that creates a {@link LogicalValues} with no rows without
 * deriving its collation trait.
 *
 * <p>{@link LogicalValues#create} derives the collation trait with {@code RelMdCollation.values},
 * which is O(n^2) in the number of fields for an empty {@code Values}. {@code RelBuilder.empty()}
 * goes through this factory, e.g. when a filter simplifies to always false
 * (opensearch-project/sql#5952). Non-empty values are created exactly as before.
 */
public final class EmptyValuesFactory implements RelFactories.ValuesFactory {

  public static final EmptyValuesFactory INSTANCE = new EmptyValuesFactory();

  private EmptyValuesFactory() {}

  @Override
  public RelNode createValues(
      RelOptCluster cluster, RelDataType rowType, List<ImmutableList<RexLiteral>> tuples) {
    if (tuples.isEmpty()) {
      return new LogicalValues(
          cluster,
          cluster.traitSetOf(Convention.NONE).replace(RelCollations.EMPTY),
          rowType,
          ImmutableList.of());
    }
    return RelFactories.DEFAULT_VALUES_FACTORY.createValues(cluster, rowType, tuples);
  }
}
