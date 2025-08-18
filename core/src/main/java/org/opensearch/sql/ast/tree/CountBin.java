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
 * AST node representing count-based bin operation. This is the third priority bin type that uses
 * "nice number" algorithm to create a specific number of bins. Supports start/end range parameters.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CountBin extends Bin {

  private final Integer bins;
  private final UnresolvedExpression start;
  private final UnresolvedExpression end;

  @Builder
  public CountBin(
      UnresolvedExpression field,
      String alias,
      Integer bins,
      UnresolvedExpression start,
      UnresolvedExpression end) {
    super(field, alias);
    this.bins = bins;
    this.start = start; // Optional parameter
    this.end = end; // Optional parameter
    validate();
  }

  @Override
  public BinType getBinType() {
    return BinType.COUNT;
  }

  @Override
  public void validate() {
    // Bins count validation based on documentation
    if (bins == null) {
      throw new IllegalArgumentException("Bins parameter is required for count-based binning");
    }
    if (bins < 2 || bins > 50000) {
      throw new IllegalArgumentException(
          String.format("The bins parameter must be between 2 and 50000, got: %d", bins));
    }
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCountBin(this, context);
  }
}
