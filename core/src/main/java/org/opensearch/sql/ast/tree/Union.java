/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/** Logical plan node for Union operation. Combines results from multiple datasets. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class Union extends UnresolvedPlan {
  /** Input datasets to union. */
  private final List<UnresolvedPlan> datasets;

  /** True for UNION (distinct), false for UNION ALL. */
  private final boolean distinct;

  /** True to merge different schemas (PPL append), false for positional union (SQL). */
  private final boolean unifySchema;

  /** Maximum rows from subsearch (PPL only, null if unlimited). */
  private final Integer maxout;

  /** SQL: UNION ALL or UNION DISTINCT (no schema unification). */
  public Union(List<UnresolvedPlan> datasets, boolean distinct) {
    this(datasets, distinct, false, null);
  }

  /** PPL: UNION ALL with maxout limit and schema unification. */
  public Union(List<UnresolvedPlan> datasets, Integer maxout) {
    this(datasets, false, true, maxout);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    List<UnresolvedPlan> newDatasets =
        ImmutableList.<UnresolvedPlan>builder().add(child).addAll(datasets).build();
    return new Union(newDatasets, distinct, unifySchema, maxout);
  }

  @Override
  public List<? extends UnresolvedPlan> getChild() {
    return datasets;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnion(this, context);
  }
}
