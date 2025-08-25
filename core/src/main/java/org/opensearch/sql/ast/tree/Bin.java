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
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Abstract AST node representing Bin operations with type-safe derived classes. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public abstract class Bin extends UnresolvedPlan {

  private UnresolvedPlan child;

  protected final UnresolvedExpression field;

  protected final Optional<String> alias;

  protected Bin(UnresolvedExpression field, Optional<String> alias) {
    this.field = field;
    this.alias = alias;
  }

  /**
   * Validates the parameters specific to this bin type. Each subclass implements its own validation
   * logic.
   */
  public abstract void validate();

  @Override
  public Bin attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitBin(this, context);
  }
}
