/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent Regex filtering operation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Regex extends UnresolvedPlan {
  /** Field to match against. */
  private final UnresolvedExpression field;

  /** Whether this is a negated match (!=). */
  private final boolean negated;

  /** Pattern. */
  private final Literal pattern;

  /** Child Plan. */
  @Setter private UnresolvedPlan child;

  public Regex(UnresolvedExpression field, String operator, Literal pattern) {
    this.field = field;
    this.negated = "!=".equals(operator);
    this.pattern = pattern;
  }

  @Override
  public Regex attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRegex(this, context);
  }
}
