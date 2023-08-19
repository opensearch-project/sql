/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.analysis;

import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.QualifierAnalyzer;
import org.opensearch.sql.analysis.SelectExpressionAnalyzer;
import org.opensearch.sql.analysis.TypeEnvironment;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.planner.logical.LogicalPlan;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OpenSearchSelectExpressionAnalyzer extends SelectExpressionAnalyzer implements OpenSearchAbstractNodeVisitor<List<NamedExpression>, AnalysisContext> {
  public OpenSearchSelectExpressionAnalyzer(OpenSearchExpressionAnalyzer expressionAnalyzer) {
    super(expressionAnalyzer);
  }

  @Override
  public List<NamedExpression> visitAlias(Alias node, AnalysisContext context) {
    // Expand all nested fields if used in SELECT clause
    if (node.getDelegated() instanceof NestedAllTupleFields) {
      return node.getDelegated().accept(this, context);
    }

    return super.visitAlias(node, context);
  }

  @Override
  public List<NamedExpression> visitNestedAllTupleFields(
      NestedAllTupleFields node, AnalysisContext context) {
    TypeEnvironment environment = context.peek();
    Map<String, ExprType> lookupAllTupleFields =
        environment.lookupAllTupleFields(Namespace.FIELD_NAME);
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
                      .accept(expressionAnalyzer, context);
              return DSL.named("nested(" + entry.getKey() + ")", nestedFunc);
            })
        .collect(Collectors.toList());
  }
}
