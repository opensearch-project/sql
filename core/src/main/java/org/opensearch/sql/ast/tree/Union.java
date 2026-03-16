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
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/** Logical plan node for Union operation. Combines results from multiple datasets (UNION ALL). */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class Union extends UnresolvedPlan {
  private final List<UnresolvedPlan> datasets;

  private Integer maxout;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    List<UnresolvedPlan> newDatasets =
        ImmutableList.<UnresolvedPlan>builder().add(child).addAll(datasets).build();
    return new Union(newDatasets, maxout);
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
