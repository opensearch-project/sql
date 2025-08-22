/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent Rex field extraction operation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Rex extends UnresolvedPlan {

  public enum RexMode {
    EXTRACT,
    SED
  }

  /** Field to extract from. */
  private final UnresolvedExpression field;

  /** Pattern with named capture groups or sed expression. */
  private final Literal pattern;

  /** Maximum number of matches (optional). */
  private final Optional<Integer> maxMatch;

  /** Offset field name for position tracking (optional). */
  private final Optional<String> offsetField;

  /** Rex mode (extract or sed). */
  private final RexMode mode;

  /** Child Plan. */
  @Setter private UnresolvedPlan child;

  public Rex(UnresolvedExpression field, Literal pattern) {
    this(field, pattern, Optional.empty(), Optional.empty(), RexMode.EXTRACT);
  }

  public Rex(UnresolvedExpression field, Literal pattern, Optional<Integer> maxMatch) {
    this(field, pattern, maxMatch, Optional.empty(), RexMode.EXTRACT);
  }

  public Rex(
      UnresolvedExpression field,
      Literal pattern,
      Optional<Integer> maxMatch,
      Optional<String> offsetField) {
    this(field, pattern, maxMatch, offsetField, RexMode.EXTRACT);
  }

  public Rex(
      UnresolvedExpression field,
      Literal pattern,
      Optional<Integer> maxMatch,
      Optional<String> offsetField,
      RexMode mode) {
    this.field = field;
    this.pattern = pattern;
    this.maxMatch = maxMatch;
    this.offsetField = offsetField;
    this.mode = mode;
  }

  @Override
  public Rex attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRex(this, context);
  }
}
