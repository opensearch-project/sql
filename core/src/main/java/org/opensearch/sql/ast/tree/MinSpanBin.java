/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
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

  @Nullable private final UnresolvedExpression start;

  @Nullable private final UnresolvedExpression end;

  @Builder
  public MinSpanBin(
      UnresolvedExpression field,
      @Nullable String alias,
      UnresolvedExpression minspan,
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
}
