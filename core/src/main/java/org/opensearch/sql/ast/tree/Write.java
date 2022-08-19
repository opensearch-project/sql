/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.QualifiedName;

/**
 * Insert statement.
 */
@Getter
@RequiredArgsConstructor
@ToString
public class Write extends UnresolvedPlan {

  private final QualifiedName tableName;

  private final List<QualifiedName> columns;

  private UnresolvedPlan child; // values or select

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitWrite(this, context);
  }

  public List<? extends Node> getChild() {
    return Collections.singletonList(child);
  }
}