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

/**
 * Utility class for resolving wildcard patterns in field names for non-Calcite analysis.
 *
 * <p>This resolver handles wildcard expansion in field expressions during analysis phase,
 * delegating all pattern matching operations to WildcardUtils for consistency. Only processes Field
 * expressions.
 *
 * <p>Used primarily by Analyzer for PROJECT operations when Calcite engine is disabled.
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
    Set<String> seenFields = new LinkedHashSet<>();

    for (UnresolvedExpression expr : projectList) {
      if (expr instanceof Field) {
        Field field = (Field) expr;
        String fieldName = field.getField().toString();

        // Use WildcardUtils for consistent wildcard detection across all components
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

  /**
   * Resolves wildcard field patterns to matching field names.
   *
   * <p>Uses WildcardUtils for pattern matching and creates ReferenceExpression objects for each
   * matching field with proper type information.
   *
   * @param fieldName Wildcard pattern to resolve
   * @param availableFields Map of available fields and their types
   * @param resolvedFields List to add resolved expressions to
   * @param seenFields Set to track already processed fields for deduplication
   */
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

  /**
   * Resolves regular (non-wildcard) field expressions.
   *
   * @param expr Field expression to analyze
   * @param fieldName Name of the field
   * @param context Analysis context
   * @param expressionAnalyzer Analyzer for expressions
   * @param resolvedFields List to add resolved expressions to
   * @param seenFields Set to track already processed fields
   */
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

  /**
   * Checks if any expression in the list contains wildcard patterns.
   *
   * @param expressions List of unresolved expressions to check
   * @return true if any expression contains wildcards, false otherwise
   */
  public static boolean hasWildcards(List<UnresolvedExpression> expressions) {
    return expressions.stream().anyMatch(WildcardFieldResolver::isWildcardField);
  }

  /**
   * Determines if an expression is a field with wildcard characters.
   *
   * <p>Delegates to WildcardUtils.containsWildcard() for consistent wildcard detection across the
   * entire codebase.
   *
   * @param expr Expression to check
   * @return true if the expression is a Field containing "*", false otherwise
   */
  public static boolean isWildcardField(UnresolvedExpression expr) {
    if (!(expr instanceof Field)) {
      return false;
    }
    String fieldName = ((Field) expr).getField().toString();
    return fieldName != null && WildcardUtils.containsWildcard(fieldName);
  }

  /**
   * Matches wildcard pattern against available fields.
   *
   * <p>Converts Set to List and delegates to WildcardUtils.expandWildcardPattern() for consistent
   * pattern matching behavior. Preserves the natural order from the available fields.
   *
   * @param pattern Wildcard pattern (supports *, prefix*, *suffix, *contains*)
   * @param availableFields Available field names as Set
   * @return List of matching field names in natural order
   */
  private static List<String> matchWildcardPattern(String pattern, Set<String> availableFields) {
    // Delegate to WildcardUtils for consistent pattern matching behavior
    return WildcardUtils.expandWildcardPattern(pattern, new ArrayList<>(availableFields));
  }
}
