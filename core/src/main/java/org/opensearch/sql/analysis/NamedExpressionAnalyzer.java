/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;

/**
 * Analyze the Alias node in the {@link AnalysisContext} to construct the list of {@link
 * NamedExpression}.
 */
@RequiredArgsConstructor
public class NamedExpressionAnalyzer extends AbstractNodeVisitor<NamedExpression, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;

  /** Analyze Select fields. */
  public NamedExpression analyze(UnresolvedExpression expression, AnalysisContext analysisContext) {
    return expression.accept(this, analysisContext);
  }

  @Override
  public NamedExpression visitAlias(Alias node, AnalysisContext context) {
    return DSL.named(
        unqualifiedNameIfFieldOnly(node, context),
        node.getDelegated().accept(expressionAnalyzer, context),
        node.getAlias());
  }

  private String unqualifiedNameIfFieldOnly(Alias node, AnalysisContext context) {
    UnresolvedExpression selectItem = node.getDelegated();
    if (selectItem instanceof QualifiedName) {
      QualifierAnalyzer qualifierAnalyzer = new QualifierAnalyzer(context);
      return qualifierAnalyzer.unqualified((QualifiedName) selectItem);
    }
    return node.getName();
  }
}
