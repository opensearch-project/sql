/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of Highlight function.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class HighlightFunction extends UnresolvedExpression {
  private final UnresolvedExpression highlightField;
  private final Map<String, Literal> arguments;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitHighlightFunction(this, context);
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return List.of(highlightField);
  }
}
