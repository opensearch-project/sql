/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/** Utility class for resolving wildcard patterns in field names for Calcite analysis only. */
public class WildcardFieldResolver {

  public static List<NamedExpression> resolveWildcards(
      List<UnresolvedExpression> projectList,
      AnalysisContext context,
      ExpressionAnalyzer expressionAnalyzer) {

    TypeEnvironment environment = context.peek();
    Map<String, ExprType> availableFields = environment.lookupAllFields(Namespace.FIELD_NAME);

    List<NamedExpression> resolvedFields = new ArrayList<>();
    Set<String> seenFields = new HashSet<>();
    boolean hasAllFields = projectList.stream().anyMatch(expr -> expr instanceof AllFields);
    boolean isPPL =
        context.getFunctionProperties() != null
            && context.getFunctionProperties().getQueryType()
                == org.opensearch.sql.executor.QueryType.PPL;

    for (UnresolvedExpression expr : projectList) {
      if (expr instanceof AllFields) {
        resolveAllFields(availableFields, resolvedFields, seenFields, isPPL);
      } else if (expr instanceof Field) {
        Field field = (Field) expr;
        String fieldName = field.getField().toString();

        if (WildcardUtils.containsWildcard(fieldName)) {
          resolveWildcardField(
              fieldName, availableFields, resolvedFields, seenFields, hasAllFields, isPPL);
        } else {
          resolveRegularField(
              expr,
              fieldName,
              context,
              expressionAnalyzer,
              resolvedFields,
              seenFields,
              hasAllFields,
              isPPL);
        }
      }
    }

    return resolvedFields;
  }

  private static void resolveAllFields(
      Map<String, ExprType> availableFields,
      List<NamedExpression> resolvedFields,
      Set<String> seenFields,
      boolean isPPL) {
    availableFields.forEach(
        (fieldName, fieldType) -> {
          if (!isPPL || !seenFields.contains(fieldName)) {
            seenFields.add(fieldName);
            resolvedFields.add(DSL.named(fieldName, new ReferenceExpression(fieldName, fieldType)));
          }
        });
  }

  private static void resolveWildcardField(
      String fieldName,
      Map<String, ExprType> availableFields,
      List<NamedExpression> resolvedFields,
      Set<String> seenFields,
      boolean hasAllFields,
      boolean isPPL) {
    List<String> matchingFields = matchWildcardPattern(fieldName, availableFields.keySet());
    for (String matchedField : matchingFields) {
      if (!isPPL || !seenFields.contains(matchedField)) {
        seenFields.add(matchedField);
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
      Set<String> seenFields,
      boolean hasAllFields,
      boolean isPPL) {
    if (!isPPL || !seenFields.contains(fieldName)) {
      seenFields.add(fieldName);
      org.opensearch.sql.expression.Expression analyzedExpr =
          expressionAnalyzer.analyze(expr, context);
      resolvedFields.add(DSL.named(analyzedExpr));
    }
  }

  public static boolean hasWildcards(List<UnresolvedExpression> expressions) {
    return expressions.stream().anyMatch(WildcardFieldResolver::isWildcardField);
  }

  public static boolean isWildcardField(UnresolvedExpression expr) {
    if (expr instanceof AllFields) {
      return true;
    }
    if (!(expr instanceof Field)) {
      return false;
    }
    return WildcardUtils.containsWildcard(((Field) expr).getField().toString());
  }

  private static List<String> matchWildcardPattern(String pattern, Set<String> availableFields) {

    return WildcardUtils.expandWildcardPattern(pattern, new ArrayList<>(availableFields));
  }
}
