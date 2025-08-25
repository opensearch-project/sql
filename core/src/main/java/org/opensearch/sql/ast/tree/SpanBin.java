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
 * AST node representing span-based bin operation. This is the highest priority bin type that uses a
 * fixed span interval. Supports aligntime parameter for time-based fields.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class SpanBin extends Bin {

  private final UnresolvedExpression span;

  private final Optional<UnresolvedExpression> aligntime; // Only valid for time-based fields

  @Builder
  public SpanBin(
      UnresolvedExpression field,
      Optional<String> alias,
      UnresolvedExpression span,
      Optional<UnresolvedExpression> aligntime) {
    super(field, alias);
    this.span = span;
    this.aligntime = aligntime;
    validate();
  }

  @Override
  public void validate() {}
}
