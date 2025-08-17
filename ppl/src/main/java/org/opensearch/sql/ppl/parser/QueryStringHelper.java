/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Helper utility for query_string function operations. Provides methods to create, validate, and
 * extract information from query_string functions.
 */
public final class QueryStringHelper {

  public static final String QUERY_STRING_FUNCTION = "query_string";
  public static final String DEFAULT_OPERATOR = "AND";

  private QueryStringHelper() {
    // Utility class - prevent instantiation
  }

  /**
   * Checks if an expression is a mergeable query_string function. Mergeable means it has standard
   * parameters (query and default_operator=AND) without custom fields or analyzers.
   */
  public static boolean isMergeableFreeText(UnresolvedExpression expr) {
    if (!(expr instanceof Function)) {
      return false;
    }

    Function func = (Function) expr;
    if (!QUERY_STRING_FUNCTION.equals(func.getFuncName())) {
      return false;
    }

    // Must have exactly 2 parameters: query and default_operator
    List<UnresolvedExpression> args = func.getFuncArgs();
    if (args.size() != 2) {
      return false;
    }

    return hasCorrectParameters(args);
  }

  private static boolean hasCorrectParameters(List<UnresolvedExpression> args) {
    boolean hasQuery = false;
    boolean hasCorrectOperator = false;

    for (UnresolvedExpression arg : args) {
      if (!(arg instanceof UnresolvedArgument)) continue;

      UnresolvedArgument unresolvedArg = (UnresolvedArgument) arg;
      String argName = unresolvedArg.getArgName();

      if ("query".equals(argName)) {
        hasQuery = true;
      } else if ("default_operator".equals(argName)) {
        hasCorrectOperator = isDefaultOperatorAnd(unresolvedArg);
      } else {
        return false; // Has other parameters
      }
    }

    return hasQuery && hasCorrectOperator;
  }

  private static boolean isDefaultOperatorAnd(UnresolvedArgument arg) {
    if (arg.getValue() instanceof Literal) {
      Object value = ((Literal) arg.getValue()).getValue();
      return DEFAULT_OPERATOR.equals(value);
    }
    return false;
  }

  /**
   * Extracts the query string parameter from a query_string function. Returns empty string if not a
   * valid query_string function.
   */
  public static String extractQuery(UnresolvedExpression expr) {
    if (!(expr instanceof Function)) {
      return "";
    }

    Function func = (Function) expr;
    for (UnresolvedExpression arg : func.getFuncArgs()) {
      if (arg instanceof UnresolvedArgument) {
        UnresolvedArgument unresolvedArg = (UnresolvedArgument) arg;
        if ("query".equals(unresolvedArg.getArgName())
            && unresolvedArg.getValue() instanceof Literal) {
          return ((Literal) unresolvedArg.getValue()).getValue().toString();
        }
      }
    }

    return "";
  }

  /** Creates a standard query_string function with the given query text. */
  public static UnresolvedExpression create(String queryString) {
    List<UnresolvedExpression> args = new ArrayList<>();
    args.add(AstDSL.unresolvedArg("query", AstDSL.stringLiteral(queryString)));
    args.add(AstDSL.unresolvedArg("default_operator", AstDSL.stringLiteral(DEFAULT_OPERATOR)));
    return AstDSL.function(QUERY_STRING_FUNCTION, args.toArray(new UnresolvedExpression[0]));
  }

  /** Checks if a query string needs parentheses when combined with other terms. */
  public static boolean needsParentheses(String query) {
    return query.contains(" OR ")
        || query.contains(" AND ")
        || query.startsWith("NOT ")
        || query.contains(" NOT ");
  }
}
