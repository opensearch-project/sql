/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/** Logical plan node of UNION RECURSIVE, the interface for recursive union queries. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class UnionRecursive extends UnresolvedPlan {

  private final String relationName;
  private final Integer maxDepth;
  private final Integer maxRows;
  private final UnresolvedPlan recursiveSubsearch;

  private UnresolvedPlan child;

  @Override
  public UnionRecursive attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitUnionRecursive(this, context);
  }
}
