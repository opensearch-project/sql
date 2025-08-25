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
 * AST node representing range-only bin operation. This is the fourth priority bin type that uses
 * effective range expansion with magnitude-based width calculation when only start/end parameters
 * are specified.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class RangeBin extends Bin {

  private final Optional<UnresolvedExpression> start;

  private final Optional<UnresolvedExpression> end;

  @Builder
  public RangeBin(
      UnresolvedExpression field,
      Optional<String> alias,
      Optional<UnresolvedExpression> start,
      Optional<UnresolvedExpression> end) {
    super(field, alias);
    this.start = start; // At least one of start/end should be specified
    this.end = end; // At least one of start/end should be specified
    validate();
  }

  @Override
  public void validate() {
    // Range-specific validation
    if (!start.isPresent() && !end.isPresent()) {
      throw new IllegalArgumentException(
          "At least one of start or end parameter must be specified for range-based binning");
    }
  }
}
