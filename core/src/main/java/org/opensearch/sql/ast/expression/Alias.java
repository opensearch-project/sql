/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

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
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class Alias extends UnresolvedExpression {

  /** The name to be associated with the result of computing delegated expression. */
  private final String name;

  /** Expression aliased. */
  private final UnresolvedExpression delegated;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAlias(this, context);
  }
}
