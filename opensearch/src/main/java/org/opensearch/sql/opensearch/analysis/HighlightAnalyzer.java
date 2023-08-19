/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.analysis;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.opensearch.ast.expression.HighlightFunction;
import org.opensearch.sql.opensearch.ast.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalPlan;

/** Analyze the highlight in the {@link AnalysisContext} to construct the {@link LogicalPlan}. */
@RequiredArgsConstructor
public class HighlightAnalyzer implements AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;
  private final LogicalPlan child;

  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan highlight = projectItem.accept(this, context);
    return (highlight == null) ? child : highlight;
  }

  @Override
  public LogicalPlan visitAlias(Alias node, AnalysisContext context) {
    UnresolvedExpression delegated = node.getDelegated();
    if (!(delegated instanceof HighlightFunction)) {
      return null;
    }

    HighlightFunction unresolved = (HighlightFunction) delegated;
    Expression field = expressionAnalyzer.analyze(unresolved.getHighlightField(), context);
    return new LogicalHighlight(child, field, unresolved.getArguments());
  }
}
