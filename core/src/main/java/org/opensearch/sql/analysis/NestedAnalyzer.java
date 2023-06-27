/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
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
      validateArgs(expressions);
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
      if (child instanceof LogicalNested) {
        ((LogicalNested)child).addFields(args);
        return child;
      } else {
        return new LogicalNested(child, new ArrayList<>(Arrays.asList(args)), namedExpressions);
      }
    }
    return null;
  }

  /**
   * Validate each parameter used in nested function in SELECT clause. Any supplied parameter
   * for a nested function in a SELECT statement must be a valid qualified name, and the field
   * parameter must be nested at least one level.
   * @param args : Arguments in nested function.
   */
  private void validateArgs(List<UnresolvedExpression> args) {
    if (args.size() < 1 || args.size() > 2) {
      throw new IllegalArgumentException(
          "on nested object only allowed 2 parameters (field,path) or 1 parameter (field)"
      );
    }

    for (int i = 0; i < args.size(); i++) {
      if (!(args.get(i) instanceof QualifiedName)) {
        throw new IllegalArgumentException(
            String.format("Illegal nested field name: %s", args.get(i).toString())
        );
      }
      if (i == 0 && ((QualifiedName)args.get(i)).getParts().size() < 2) {
        throw new IllegalArgumentException(
            String.format("Illegal nested field name: %s", args.get(i).toString())
        );
      }
    }
  }

  /**
   * Generate nested path dynamically. Assumes at least one level of nesting in supplied string.
   * @param field : Nested field to generate path of.
   * @return : Path of field derived from last level of nesting.
   */
  public static ReferenceExpression generatePath(String field) {
    return new ReferenceExpression(field.substring(0, field.lastIndexOf(".")), STRING);
  }

  /**
   * Check if supplied expression is a nested function.
   * @param expr Expression checking if is nested function.
   * @return True if expression is a nested function.
   */
  public static Boolean isNestedFunction(Expression expr) {
    return (expr instanceof FunctionExpression
        && ((FunctionExpression) expr).getFunctionName().getFunctionName()
        .equalsIgnoreCase(BuiltinFunctionName.NESTED.name()));
  }
}
