/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/** Utility class for resolving wildcard patterns in field names for non-Calcite analysis. */
public class WildcardFieldResolver {

  public static List<NamedExpression> resolveWildcards(
      List<UnresolvedExpression> projectList,
      AnalysisContext context,
      ExpressionAnalyzer expressionAnalyzer) {

    TypeEnvironment environment = context.peek();
    Map<String, ExprType> availableFields = environment.lookupAllFields(Namespace.FIELD_NAME);

    List<NamedExpression> resolvedFields = new ArrayList<>();
    Set<String> seenFields = new LinkedHashSet<>();

    for (UnresolvedExpression expr : projectList) {
      if (expr instanceof Field) {
        Field field = (Field) expr;
        String fieldName = field.getField().toString();

        if (WildcardUtils.containsWildcard(fieldName)) {
          resolveWildcardField(fieldName, availableFields, resolvedFields, seenFields);
        } else {
          resolveRegularField(
              expr, fieldName, context, expressionAnalyzer, resolvedFields, seenFields);
        }
      }
    }

    return resolvedFields;
  }

  private static void resolveWildcardField(
      String fieldName,
      Map<String, ExprType> availableFields,
      List<NamedExpression> resolvedFields,
      Set<String> seenFields) {
    List<String> matchingFields = matchWildcardPattern(fieldName, availableFields.keySet());
    for (String matchedField : matchingFields) {
      if (seenFields.add(matchedField)) {
        ExprType fieldType = availableFields.get(matchedField);
        resolvedFields.add(
            DSL.named(matchedField, new ReferenceExpression(matchedField, fieldType)));
      }
    }
  }

  private static void resolveRegularField(
      UnresolvedExpression expr,
      String fieldName,
      AnalysisContext context,
      ExpressionAnalyzer expressionAnalyzer,
      List<NamedExpression> resolvedFields,
      Set<String> seenFields) {
    if (seenFields.add(fieldName)) {
      org.opensearch.sql.expression.Expression analyzedExpr =
          expressionAnalyzer.analyze(expr, context);
      resolvedFields.add(DSL.named(analyzedExpr));
    }
  }

  public static boolean hasWildcards(List<UnresolvedExpression> expressions) {
    return expressions.stream().anyMatch(WildcardFieldResolver::isWildcardField);
  }

  public static boolean isWildcardField(UnresolvedExpression expr) {
    if (!(expr instanceof Field)) {
      return false;
    }
    String fieldName = ((Field) expr).getField().toString();
    return fieldName != null && WildcardUtils.containsWildcard(fieldName);
  }

  private static List<String> matchWildcardPattern(String pattern, Set<String> availableFields) {

    return WildcardUtils.expandWildcardPattern(pattern, new ArrayList<>(availableFields));
  }
}
