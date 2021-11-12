/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of binary operator or comparison relation EQUAL.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class EqualTo extends UnresolvedExpression {
  @Getter
  private UnresolvedExpression left;
  @Getter
  private UnresolvedExpression right;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(left, right);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitEqualTo(this, context);
  }
}
