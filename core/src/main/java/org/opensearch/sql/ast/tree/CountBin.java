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
import org.opensearch.sql.calcite.utils.binning.BinConstants;

/**
 * AST node representing count-based bin operation. This is the third priority bin type that uses
 * "nice number" algorithm to create a specific number of bins. Supports start/end range parameters.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CountBin extends Bin {

  private final Integer bins;

  @Nullable private final UnresolvedExpression start;

  @Nullable private final UnresolvedExpression end;

  @Builder
  public CountBin(
      UnresolvedExpression field,
      @Nullable String alias,
      Integer bins,
      @Nullable UnresolvedExpression start,
      @Nullable UnresolvedExpression end) {
    super(field, alias);
    this.bins = bins;
    this.start = start;
    this.end = end;
    validate();
  }

  @Override
  public void validate() {
    // Bins count validation based on documentation
    if (bins < BinConstants.MIN_BINS || bins > BinConstants.MAX_BINS) {
      throw new IllegalArgumentException(
          String.format(
              "The bins parameter must be between %d and %d, got: %d",
              BinConstants.MIN_BINS, BinConstants.MAX_BINS, bins));
    }
  }
}
