/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.opensearch.analysis.OpenSearchAbstractNodeVisitor;

/** Represents all tuple fields used in nested function. */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class NestedAllTupleFields extends UnresolvedExpression {
  @Getter private final String path;

  @Override
  public List<? extends Node> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    if (nodeVisitor instanceof OpenSearchAbstractNodeVisitor) {
      return ((OpenSearchAbstractNodeVisitor<T, C>)nodeVisitor).visitNestedAllTupleFields(this, context);
    }
    return null;
  }

  @Override
  public String toString() {
    return String.format("nested(%s.*)", path);
  }
}
