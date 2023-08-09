/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST Node for Table Function. */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class TableFunction extends UnresolvedPlan {

  private final UnresolvedExpression functionName;

  @Getter private final List<UnresolvedExpression> arguments;

  public QualifiedName getFunctionName() {
    return (QualifiedName) functionName;
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
