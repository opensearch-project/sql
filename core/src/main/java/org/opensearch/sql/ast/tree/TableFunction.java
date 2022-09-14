/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Logical plan node of Relation, the interface for building the searching sources.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class TableFunction extends UnresolvedPlan {

  private final UnresolvedExpression tableFunction;

  public UnresolvedExpression getTableFunction() {
    return tableFunction;
  }


  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitTableFunction(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return null;
  }
}
