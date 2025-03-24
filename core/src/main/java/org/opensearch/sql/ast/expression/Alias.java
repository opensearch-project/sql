/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Alias abstraction that associate an unnamed expression with a name. The name information
 * preserved is useful for semantic analysis and response formatting eventually. This can avoid
 * restoring the info in toString() method which is inaccurate because original info is already
 * lost.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class Alias extends UnresolvedExpression {

  /**
   * The name to be associated with the result of computing delegated expression. In OpenSearch ppl,
   * the name is the actual alias of an expression
   */
  private final String name;

  /** Expression aliased. */
  private final UnresolvedExpression delegated;

  /** TODO. Optional field alias. This field is OpenSearch SQL-only */
  private String alias;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAlias(this, context);
  }
}
