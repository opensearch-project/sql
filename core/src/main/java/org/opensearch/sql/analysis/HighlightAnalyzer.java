package org.opensearch.sql.analysis;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalPlan;

@RequiredArgsConstructor
public class HighlightAnalyzer  extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;
  private final LogicalPlan child;

  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan highlight = projectItem.accept(this, context);
    return (highlight == null) ? child : highlight;
  }

  @Override
  public LogicalPlan visitAlias(Alias node, AnalysisContext context) {
    if (!(node.getDelegated() instanceof HighlightFunction)) {
      return null;
    }

    HighlightFunction unresolved = (HighlightFunction) node.getDelegated();
    Expression field = expressionAnalyzer.analyze(unresolved.getHighlightField(), context);
    return new LogicalHighlight(child, field);
  }
}
