/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.List;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node that holds a function which should be replaced by its constant[1] value.
 * [1] Constant at execution time.
 */
@EqualsAndHashCode(callSuper = false)
public class ConstantFunction extends Function {

  public ConstantFunction(String funcName, List<UnresolvedExpression> funcArgs) {
    super(funcName, funcArgs);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitConstantFunction(this, context);
  }
}
