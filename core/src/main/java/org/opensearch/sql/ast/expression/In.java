/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import static java.util.Collections.singletonList;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of one-to-many mapping relation IN. Params include the field expression and/or
 * wildcard field expression, nested field expression (@field). And the values that the field is
 * mapped to (@valueList).
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class In extends UnresolvedExpression {
  private final UnresolvedExpression field;
  private final List<UnresolvedExpression> valueList;

  @Override
  public List<UnresolvedExpression> getChild() {
    return singletonList(field);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitIn(this, context);
  }
}
