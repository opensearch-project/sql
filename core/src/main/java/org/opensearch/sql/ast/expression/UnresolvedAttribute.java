/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node, representing the syntax that is not resolved to
 * any other expression nodes yet but non-negligible
 * This expression is often created as the index name, field name etc.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@Getter
public class UnresolvedAttribute extends UnresolvedExpression {
  private final String attr;

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnresolvedAttribute(this, context);
  }
}
