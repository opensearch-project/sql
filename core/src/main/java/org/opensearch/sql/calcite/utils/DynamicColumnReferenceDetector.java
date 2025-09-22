/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.List;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.CalcitePlanContext;

/**
 * AST visitor to detect if expressions reference dynamic columns that would require the
 * _dynamic_columns MAP field. This replaces fragile toString() based detection with proper AST
 * analysis.
 */
public class DynamicColumnReferenceDetector
    extends AbstractNodeVisitor<Boolean, CalcitePlanContext> {

  /**
   * Main entry point to check if an expression contains references to dynamic columns.
   *
   * @param expr The expression to analyze
   * @param context The Calcite plan context containing schema information
   * @return true if the expression references dynamic columns, false otherwise
   */
  public static boolean containsDynamicColumnReference(
      UnresolvedExpression expr, CalcitePlanContext context) {
    DynamicColumnReferenceDetector detector = new DynamicColumnReferenceDetector();
    return isTrue(expr.accept(detector, context));
  }

  /**
   * Check if multiple expressions contain dynamic column references.
   *
   * @param expressions List of expressions to analyze
   * @param context The Calcite plan context
   * @return true if any expression references dynamic columns
   */
  public static boolean containsDynamicColumnReference(
      List<UnresolvedExpression> expressions, CalcitePlanContext context) {
    return expressions.stream().anyMatch(expr -> containsDynamicColumnReference(expr, context));
  }

  @Override
  public Boolean visitQualifiedName(QualifiedName node, CalcitePlanContext context) {
    return node.getParts().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);
  }

  @Override
  public Boolean visitField(Field node, CalcitePlanContext context) {
    // Field wraps a QualifiedName, so delegate to it
    return isTrue(node.getField().accept(this, context));
  }

  @Override
  public Boolean visitAlias(Alias node, CalcitePlanContext context) {
    // Check the delegated expression
    return isTrue(node.getDelegated().accept(this, context));
  }

  @Override
  public Boolean visitLet(Let node, CalcitePlanContext context) {
    // Check the expression being assigned
    return isTrue(node.getExpression().accept(this, context));
  }

  @Override
  public Boolean visitFunction(Function node, CalcitePlanContext context) {
    // Check if any function arguments reference dynamic columns
    return node.getFuncArgs().stream()
        .anyMatch(
            arg -> {
              return isTrue(arg.accept(this, context));
            });
  }

  @Override
  public Boolean visitLiteral(Literal node, CalcitePlanContext context) {
    // Literals never reference dynamic columns
    return false;
  }

  // Override visitChildren to provide default false behavior for unhandled node types
  @Override
  public Boolean visitChildren(org.opensearch.sql.ast.Node node, CalcitePlanContext context) {
    // For any unhandled node types, check if any child expressions reference dynamic columns
    return node.getChild().stream()
        .filter(child -> child instanceof UnresolvedExpression)
        .map(child -> (UnresolvedExpression) child)
        .anyMatch(
            child -> {
              return isTrue(child.accept(this, context));
            });
  }

  private static boolean isTrue(Boolean value) {
    return value != null && value;
  }
}
