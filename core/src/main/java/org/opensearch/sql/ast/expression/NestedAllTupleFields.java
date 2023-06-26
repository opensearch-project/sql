/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * Represents all tuple fields used in nested function.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class NestedAllTupleFields extends UnresolvedExpression {
  @Getter
  private final String path;

  @Override
  public List<? extends Node> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitNestedAllTupleFields(this, context);
  }

  @Override
  public String toString() {
    return String.format("nested(%s.*)", path);
  }
}
