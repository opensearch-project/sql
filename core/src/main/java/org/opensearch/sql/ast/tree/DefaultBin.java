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
 * AST node representing default magnitude-based bin operation. This is the lowest priority bin type
 * that uses automatic magnitude-based algorithm when no explicit binning parameters are specified.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class DefaultBin extends Bin {

  @Builder
  public DefaultBin(UnresolvedExpression field, String alias) {
    super(field, alias);
    validate();
  }

  @Override
  public BinType getBinType() {
    return BinType.DEFAULT;
  }

  @Override
  public void validate() {
    // Default bin has no additional parameters to validate
    // Field validation is already handled in the base class
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitDefaultBin(this, context);
  }
}
