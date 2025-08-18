/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node representing minimum span-based bin operation. This is the second priority bin type that
 * uses magnitude-based algorithm with minimum span constraint. Supports start/end range parameters.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MinSpanBin extends Bin {

  private final UnresolvedExpression minspan;
  private final UnresolvedExpression start;
  private final UnresolvedExpression end;

  @Builder
  public MinSpanBin(
      UnresolvedExpression field,
      String alias,
      UnresolvedExpression minspan,
      UnresolvedExpression start,
      UnresolvedExpression end) {
    super(field, alias);
    this.minspan = Objects.requireNonNull(minspan, "MinSpan cannot be null for MinSpanBin");
    this.start = start; // Optional parameter
    this.end = end; // Optional parameter
    validate();
  }

  @Override
  public BinType getBinType() {
    return BinType.MIN_SPAN;
  }

  @Override
  public void validate() {
    // MinSpan-specific validation
    if (minspan == null) {
      throw new IllegalArgumentException("MinSpan parameter is required for minspan-based binning");
    }
    // Additional validation can be added here for minspan value constraints (must be positive)
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMinSpanBin(this, context);
  }
}
