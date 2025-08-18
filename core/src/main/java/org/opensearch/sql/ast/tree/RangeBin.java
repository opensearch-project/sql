/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node representing range-only bin operation. This is the fourth priority bin type that uses
 * effective range expansion with magnitude-based width calculation when only start/end parameters
 * are specified.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class RangeBin extends Bin {

  private final UnresolvedExpression start;
  private final UnresolvedExpression end;

  @Builder
  public RangeBin(
      UnresolvedExpression field,
      String alias,
      UnresolvedExpression start,
      UnresolvedExpression end) {
    super(field, alias);
    this.start = start; // At least one of start/end should be specified
    this.end = end; // At least one of start/end should be specified
    validate();
  }

  @Override
  public BinType getBinType() {
    return BinType.RANGE;
  }

  @Override
  public void validate() {
    // Range-specific validation
    if (start == null && end == null) {
      throw new IllegalArgumentException(
          "At least one of start or end parameter must be specified for range-based binning");
    }
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRangeBin(this, context);
  }
}
