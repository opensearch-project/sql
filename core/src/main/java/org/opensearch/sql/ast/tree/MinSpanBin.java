/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Optional;
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

  private final Optional<UnresolvedExpression> start;

  private final Optional<UnresolvedExpression> end;

  @Builder
  public MinSpanBin(
      UnresolvedExpression field,
      Optional<String> alias,
      UnresolvedExpression minspan,
      Optional<UnresolvedExpression> start,
      Optional<UnresolvedExpression> end) {
    super(field, alias);
    this.minspan = minspan;
    this.start = start;
    this.end = end;
    validate();
  }

  @Override
  public void validate() {}
}
