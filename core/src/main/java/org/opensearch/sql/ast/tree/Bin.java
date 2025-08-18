/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
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
  protected final String alias;

  protected Bin(UnresolvedExpression field, String alias) {
    this.field = Objects.requireNonNull(field, "Field cannot be null");
    this.alias = alias;
  }

  /**
   * Returns the specific type of bin operation.
   *
   * @return the bin type for visitor dispatching
   */
  public abstract BinType getBinType();

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

  /** Enumeration of bin operation types for visitor dispatching. */
  public enum BinType {
    SPAN, // span parameter (highest priority)
    MIN_SPAN, // minspan parameter (second priority)
    COUNT, // bins parameter (third priority)
    RANGE, // start/end only (fourth priority)
    DEFAULT // no parameters (default magnitude-based)
  }
}
