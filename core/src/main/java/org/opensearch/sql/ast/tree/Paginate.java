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
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/** AST node to represent pagination operation. Actually a wrapper to the AST. */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class Paginate extends UnresolvedPlan {
  @Getter private final int pageSize;
  private UnresolvedPlan child;

  public Paginate(int pageSize, UnresolvedPlan child) {
    this.pageSize = pageSize;
    this.child = child;
  }

  @Override
  public List<? extends Node> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitPaginate(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }
}
