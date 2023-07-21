/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import java.util.List;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * AST node to represent close cursor operation.
 * Actually a wrapper to the AST.
 */
public class CloseCursor extends UnresolvedPlan {

  /**
   * An instance of {@link FetchCursor}.
   */
  private UnresolvedPlan cursor;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCloseCursor(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.cursor = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return List.of(cursor);
  }
}
