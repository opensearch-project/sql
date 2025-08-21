/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
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

  @NonNull private final UnresolvedExpression minspan;

  @Nullable private final UnresolvedExpression start;

  @Nullable private final UnresolvedExpression end;

  @Builder
  public MinSpanBin(
      @NonNull UnresolvedExpression field,
      @Nullable String alias,
      @NonNull UnresolvedExpression minspan,
      @Nullable UnresolvedExpression start,
      @Nullable UnresolvedExpression end) {
    super(field, alias);
    this.minspan = minspan;
    this.start = start;
    this.end = end;
    validate();
  }

  @Override
  public void validate() {}

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMinSpanBin(this, context);
  }
}
