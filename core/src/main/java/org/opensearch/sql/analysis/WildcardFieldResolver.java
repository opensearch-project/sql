/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Utility class for resolving wildcard patterns in field names for table and fields commands when
 * Calcite engine is not enabled.
 */
public class WildcardFieldResolver {

  /**
   * Resolves wildcard patterns in field expressions to actual field names from the type
   * environment.
   *
   * @param projectList List of unresolved expressions that may contain wildcards
   * @param context Analysis context containing type environment
   * @param expressionAnalyzer Expression analyzer for non-wildcard expressions
   * @return List of named expressions with wildcards resolved to actual field names
   */
  public static List<NamedExpression> resolveWildcards(
      List<UnresolvedExpression> projectList,
      AnalysisContext context,
      ExpressionAnalyzer expressionAnalyzer) {

    TypeEnvironment environment = context.peek();
    Map<String, ExprType> availableFields = environment.lookupAllFields(Namespace.FIELD_NAME);

    List<NamedExpression> resolvedFields = new ArrayList<>();
    java.util.Set<String> seenFields = new java.util.LinkedHashSet<>();

    for (UnresolvedExpression expr : projectList) {
      if (expr instanceof Field) {
        Field field = (Field) expr;
        String fieldName = field.getField().toString();

        if (isWildcardPattern(fieldName)) {
          // Resolve wildcard pattern to matching field names
          List<String> matchingFields = matchWildcardPattern(fieldName, availableFields.keySet());
          for (String matchedField : matchingFields) {
            if (!seenFields.contains(matchedField)) {
              ExprType fieldType = availableFields.get(matchedField);
              resolvedFields.add(
                  DSL.named(matchedField, new ReferenceExpression(matchedField, fieldType)));
              seenFields.add(matchedField);
            }
          }
        } else {
          // Regular field, no wildcard - use expression analyzer
          if (!seenFields.contains(fieldName)) {
            org.opensearch.sql.expression.Expression analyzedExpr =
                expressionAnalyzer.analyze(expr, context);
            resolvedFields.add(DSL.named(analyzedExpr));
            seenFields.add(fieldName);
          }
        }
      } else {
        // Non-field expressions (aliases, functions, etc.) - use expression analyzer
        org.opensearch.sql.expression.Expression analyzedExpr =
            expressionAnalyzer.analyze(expr, context);
        resolvedFields.add(DSL.named(analyzedExpr));
      }
    }

    return resolvedFields;
  }

  /**
   * Checks if a field name contains wildcard patterns (* character).
   *
   * @param fieldName The field name to check
   * @return true if the field name contains wildcards, false otherwise
   */
  private static boolean isWildcardPattern(String fieldName) {
    return fieldName.contains("*");
  }

  /**
   * Matches a wildcard pattern against available field names. Supports prefix (*name), suffix
   * (account*), and contains (*a*) patterns.
   *
   * @param pattern The wildcard pattern to match
   * @param availableFields Set of available field names
   * @return List of field names that match the pattern
   */
  private static List<String> matchWildcardPattern(
      String pattern, java.util.Set<String> availableFields) {
    // Convert wildcard pattern to regex
    String regexPattern = pattern.replace("*", ".*");
    Pattern compiledPattern = Pattern.compile("^" + regexPattern + "$");

    return availableFields.stream()
        .filter(fieldName -> compiledPattern.matcher(fieldName).matches())
        .sorted() // Maintain consistent ordering
        .collect(Collectors.toList());
  }
}
