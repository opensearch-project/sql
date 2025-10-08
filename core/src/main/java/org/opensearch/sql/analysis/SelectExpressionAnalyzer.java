/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Analyze the select list in the {@link AnalysisContext} to construct the list of {@link
 * NamedExpression}.
 */
@RequiredArgsConstructor
public class SelectExpressionAnalyzer
    extends AbstractNodeVisitor<List<NamedExpression>, SelectExpressionAnalyzer.AnalysisContextWithOptimizer> {
  private final ExpressionAnalyzer expressionAnalyzer;

  /** Analyze Select fields. */
  public List<NamedExpression> analyze(
      List<UnresolvedExpression> selectList,
      AnalysisContext analysisContext,
      ExpressionReferenceOptimizer optimizer) {
    // Create per-request context wrapper to avoid shared mutable state
    AnalysisContextWithOptimizer contextWithOptimizer = new AnalysisContextWithOptimizer(analysisContext, optimizer);
    ImmutableList.Builder<NamedExpression> builder = new ImmutableList.Builder<>();
    for (UnresolvedExpression unresolvedExpression : selectList) {
      builder.addAll(unresolvedExpression.accept(this, contextWithOptimizer));
    }
    return builder.build();
  }

  /** Context wrapper to pass optimizer per-request without shared state */
  static class AnalysisContextWithOptimizer {
    final AnalysisContext analysisContext;
    final ExpressionReferenceOptimizer optimizer;
    
    AnalysisContextWithOptimizer(AnalysisContext analysisContext, ExpressionReferenceOptimizer optimizer) {
      this.analysisContext = analysisContext;
      this.optimizer = optimizer;
    }
  }

  @Override
  public List<NamedExpression> visitField(Field node, AnalysisContextWithOptimizer context) {
    return Collections.singletonList(DSL.named(node.accept(expressionAnalyzer, context.analysisContext)));
  }

  @Override
  public List<NamedExpression> visitAlias(Alias node, AnalysisContextWithOptimizer context) {
    // Expand all nested fields if used in SELECT clause
    if (node.getDelegated() instanceof NestedAllTupleFields) {
      return node.getDelegated().accept(this, context);
    }

    Expression expr = referenceIfSymbolDefined(node, context);
    return Collections.singletonList(
        DSL.named(unqualifiedNameIfFieldOnly(node, context), expr, node.getAlias()));
  }

  /**
   * The Alias could be
   *
   * <ol>
   *   <li>SELECT name, AVG(age) FROM s BY name -> Project(Alias("name", expr), Alias("AVG(age)",
   *       aggExpr)) Agg(Alias("AVG(age)", aggExpr))
   *   <li>SELECT length(name), AVG(age) FROM s BY length(name) Project(Alias("name", expr),
   *       Alias("AVG(age)", aggExpr)) Agg(Alias("AVG(age)", aggExpr))
   *   <li>SELECT length(name) as l, AVG(age) FROM s BY l Project(Alias("name", expr, l),
   *       Alias("AVG(age)", aggExpr)) Agg(Alias("AVG(age)", aggExpr), Alias("length(name)",
   *       groupExpr))
   * </ol>
   */
  private Expression referenceIfSymbolDefined(Alias expr, AnalysisContextWithOptimizer context) {
    UnresolvedExpression delegatedExpr = expr.getDelegated();

    // Pass named expression because expression like window function loses full name
    // (OVER clause) and thus depends on name in alias to be replaced correctly
    return context.optimizer.optimize(
        DSL.named(
            expr.getName(), delegatedExpr.accept(expressionAnalyzer, context.analysisContext), expr.getAlias()),
        context.analysisContext);
  }

  @Override
  public List<NamedExpression> visitAllFields(AllFields node, AnalysisContextWithOptimizer context) {
    TypeEnvironment environment = context.analysisContext.peek();
    Map<String, ExprType> lookupAllFields = environment.lookupAllFields(Namespace.FIELD_NAME);
    return lookupAllFields.entrySet().stream()
        .map(
            entry ->
                DSL.named(
                    entry.getKey(), new ReferenceExpression(entry.getKey(), entry.getValue())))
        .collect(Collectors.toList());
  }

  @Override
  public List<NamedExpression> visitNestedAllTupleFields(
      NestedAllTupleFields node, AnalysisContextWithOptimizer context) {
    TypeEnvironment environment = context.analysisContext.peek();
    Map<String, ExprType> lookupAllTupleFields = environment.lookupAllTupleFields(Namespace.FIELD_NAME);
    environment.resolve(new Symbol(Namespace.FIELD_NAME, node.getPath()));

    // Match all fields with same path as used in nested function.
    Pattern p = Pattern.compile(node.getPath() + "\\.[^\\.]+$");
    return lookupAllTupleFields.entrySet().stream()
        .filter(field -> p.matcher(field.getKey()).find())
        .map(
            entry -> {
              Expression nestedFunc =
                  new Function(
                          "nested",
                          List.of(new QualifiedName(List.of(entry.getKey().split("\\.")))))
                      .accept(expressionAnalyzer, context.analysisContext);
              return DSL.named("nested(" + entry.getKey() + ")", nestedFunc);
            })
        .collect(Collectors.toList());
  }

  /**
   * Get unqualified name if select item is just a field. For example, suppose an index named
   * "accounts", return "age" for "SELECT accounts.age". But do nothing for expression in "SELECT
   * ABS(accounts.age)". Note that an assumption is made implicitly that original name field in
   * Alias must be the same as the values in QualifiedName. This is true because AST builder does
   * this. Otherwise, what unqualified() returns will override Alias's name as NamedExpression's
   * name even though the QualifiedName doesn't have qualifier.
   */
  private String unqualifiedNameIfFieldOnly(Alias node, AnalysisContextWithOptimizer context) {
    UnresolvedExpression selectItem = node.getDelegated();
    if (selectItem instanceof QualifiedName) {
      QualifierAnalyzer qualifierAnalyzer = new QualifierAnalyzer(context.analysisContext);
      return qualifierAnalyzer.unqualified((QualifiedName) selectItem);
    }
    return node.getName();
  }
}