/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * Analyze the Nested Function in the {@link AnalysisContext} to construct the {@link
 * LogicalPlan}.
 */
@RequiredArgsConstructor
public class NestedAnalyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
  private final List<NamedExpression> namedExpressions;
  private final ExpressionAnalyzer expressionAnalyzer;
  private final LogicalPlan child;

  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan nested = projectItem.accept(this, context);
    return (nested == null) ? child : nested;
  }

  @Override
  public LogicalPlan visitAlias(Alias node, AnalysisContext context) {
    return node.getDelegated().accept(this, context);
  }

  @Override
  public LogicalPlan visitFunction(Function node, AnalysisContext context) {
    if (node.getFuncName().equalsIgnoreCase(BuiltinFunctionName.NESTED.name())) {

      List<UnresolvedExpression> expressions = node.getFuncArgs();
      ReferenceExpression nestedField =
          (ReferenceExpression)expressionAnalyzer.analyze(expressions.get(0), context);
      Map<String, ReferenceExpression> args;
      if (expressions.size() == 2) {
        args = Map.of(
            "field", nestedField,
            "path", (ReferenceExpression)expressionAnalyzer.analyze(expressions.get(1), context)
        );
      } else {
        args = Map.of(
            "field", (ReferenceExpression)expressionAnalyzer.analyze(expressions.get(0), context),
            "path", generatePath(nestedField.toString())
        );
      }
      return new LogicalNested(child, List.of(args), namedExpressions);
    }
    return null;
  }

  private ReferenceExpression generatePath(String field) {
    return new ReferenceExpression(field.substring(0, field.lastIndexOf(".")), STRING);
  }
}
