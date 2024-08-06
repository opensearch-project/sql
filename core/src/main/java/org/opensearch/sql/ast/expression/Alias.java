/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * Alias abstraction that associate an unnamed expression with a name and an optional alias. The
 * name and alias information preserved is useful for semantic analysis and response formatting
 * eventually. This can avoid restoring the info in toString() method which is inaccurate because
 * original info is already lost.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class Alias extends UnresolvedExpression {

  /** Original field name. */
  private final String name;

  /** Expression aliased. */
  private final UnresolvedExpression delegated;

  /** Optional field alias. */
  private String alias;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAlias(this, context);
  }

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of(this.delegated);
  }
}
