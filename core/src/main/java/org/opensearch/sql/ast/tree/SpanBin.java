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
 * AST node representing span-based bin operation. This is the highest priority bin type that uses a
 * fixed span interval. Supports aligntime parameter for time-based fields.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class SpanBin extends Bin {

  private final UnresolvedExpression span;
  private final UnresolvedExpression aligntime; // Only valid for time-based fields

  @Builder
  public SpanBin(
      UnresolvedExpression field,
      String alias,
      UnresolvedExpression span,
      UnresolvedExpression aligntime) {
    super(field, alias);
    this.span = Objects.requireNonNull(span, "Span cannot be null for SpanBin");
    this.aligntime = aligntime; // Optional parameter
    validate();
  }

  @Override
  public BinType getBinType() {
    return BinType.SPAN;
  }

  @Override
  public void validate() {
    // Span-specific validation
    if (span == null) {
      throw new IllegalArgumentException("Span parameter is required for span-based binning");
    }
    // Additional validation can be added here for span value constraints
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitSpanBin(this, context);
  }
}
