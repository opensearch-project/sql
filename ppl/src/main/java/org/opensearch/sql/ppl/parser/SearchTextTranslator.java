/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.*;

/**
 * Translates and optimizes free text search expressions into OpenSearch query_string functions.
 *
 * <p>The optimizer groups multiple query_string functions together to reduce execution cost.
 *
 * <p>Example optimization:
 *
 * <pre>
 * Input:  query_string("term1") AND (status=200 AND query_string("term2"))
 * Output: query_string("term1 AND term2") AND status=200
 * </pre>
 */
public class SearchTextTranslator {

  private static final String CHARS_TO_ESCAPE = "+-=><![]{^~:\\/";

  /** Translates and optimizes a list of expressions. */
  public UnresolvedExpression translate(List<UnresolvedExpression> expressions) {
    if (expressions.isEmpty()) {
      return null;
    }

    // Combine multiple expressions with implicit AND
    UnresolvedExpression combined =
        expressions.size() == 1
            ? expressions.get(0)
            : expressions.stream().reduce(AstDSL::and).orElse(null);

    return optimizeQueryStrings(combined);
  }

  /**
   * Single-pass optimization: Convert FreeText to query_string and merge adjacent query_strings.
   */
  private UnresolvedExpression optimizeQueryStrings(UnresolvedExpression expr) {
    // Convert FreeTextExpression to query_string function
    if (expr instanceof FreeTextExpression) {
      return createQueryString((FreeTextExpression) expr);
    }

    // Handle NOT - if inner is query_string, merge NOT into it
    if (expr instanceof Not) {
      Not not = (Not) expr;
      UnresolvedExpression inner = optimizeQueryStrings(not.getExpression());

      if (QueryStringHelper.isMergeableFreeText(inner)) {
        String query = QueryStringHelper.extractQuery(inner);
        return QueryStringHelper.create("NOT " + query);
      }

      return AstDSL.not(inner);
    }

    // Handle AND
    if (expr instanceof And) {
      List<UnresolvedExpression> terms = new ArrayList<>();
      collectAndTerms(expr, terms);
      return processTermsAndBuildResult(terms, "AND");
    }

    // Handle OR
    if (expr instanceof Or) {
      List<UnresolvedExpression> terms = new ArrayList<>();
      collectOrTerms(expr, terms);
      return processTermsAndBuildResult(terms, "OR");
    }

    return expr;
  }

  /** Process terms by separating query_strings from other expressions and build result. */
  private UnresolvedExpression processTermsAndBuildResult(
      List<UnresolvedExpression> terms, String operator) {

    List<String> queryStrings = new ArrayList<>();
    List<UnresolvedExpression> others = new ArrayList<>();

    for (UnresolvedExpression term : terms) {
      UnresolvedExpression optimized = optimizeQueryStrings(term);
      if (QueryStringHelper.isMergeableFreeText(optimized)) {
        queryStrings.add(QueryStringHelper.extractQuery(optimized));
      } else {
        others.add(optimized);
      }
    }

    return buildResult(queryStrings, others, operator);
  }

  /** Flatten nested AND expressions to collect all terms. */
  private void collectAndTerms(UnresolvedExpression expr, List<UnresolvedExpression> terms) {
    if (expr instanceof And) {
      And and = (And) expr;
      collectAndTerms(and.getLeft(), terms);
      collectAndTerms(and.getRight(), terms);
    } else {
      terms.add(expr);
    }
  }

  /** Flatten nested OR expressions to collect all terms. */
  private void collectOrTerms(UnresolvedExpression expr, List<UnresolvedExpression> terms) {
    if (expr instanceof Or) {
      Or or = (Or) expr;
      collectOrTerms(or.getLeft(), terms);
      collectOrTerms(or.getRight(), terms);
    } else {
      terms.add(expr);
    }
  }

  /** Build the final result by combining query_strings and other terms. */
  private UnresolvedExpression buildResult(
      List<String> queryStrings, List<UnresolvedExpression> others, String operator) {

    List<UnresolvedExpression> allTerms = new ArrayList<>();

    // Merge all query_strings into one if we have any
    if (!queryStrings.isEmpty()) {
      String merged;
      if (queryStrings.size() == 1) {
        merged = queryStrings.get(0);
      } else {
        merged = "(" + String.join(" " + operator + " ", queryStrings) + ")";
      }
      allTerms.add(QueryStringHelper.create(merged));
    }

    // Add other terms
    allTerms.addAll(others);

    // Combine all terms
    if (allTerms.isEmpty()) {
      return null;
    }
    if (allTerms.size() == 1) {
      return allTerms.get(0);
    }

    return "AND".equals(operator)
        ? allTerms.stream().reduce(AstDSL::and).orElse(null)
        : allTerms.stream().reduce(AstDSL::or).orElse(null);
  }

  /** Create query_string function from FreeTextExpression. */
  private UnresolvedExpression createQueryString(FreeTextExpression freeText) {
    UnresolvedExpression inner = freeText.getExpression();

    if (inner instanceof Literal) {
      String queryText = convertLiteralToQueryString((Literal) inner);
      return QueryStringHelper.create(queryText);
    }

    return QueryStringHelper.create(inner.toString());
  }

  /** Convert literal to query string format with minimal escaping. */
  private String convertLiteralToQueryString(Literal literal) {
    Object value = literal.getValue();
    if (!(value instanceof String)) {
      return value.toString();
    }

    String strValue = (String) value;

    // Handle phrases with spaces - add quotes
    if (strValue.contains(" ")) {
      String escaped = escapeSpecialChars(strValue);
      return "\"" + escaped.replace("\"", "\\\"") + "\"";
    }

    // Check if we need quotes for special characters
    if (containsSpecialChars(strValue)) {
      String escaped = escapeSpecialChars(strValue);
      return "\"" + escaped.replace("\"", "\\\"") + "\"";
    }

    // Simple term - just escape reserved characters
    return escapeSpecialChars(strValue);
  }

  /** Check if string contains special characters that need quoting. */
  private boolean containsSpecialChars(String text) {
    String specialChars = "+=<>!(){}[]^~:\\/";
    for (char c : text.toCharArray()) {
      if (specialChars.indexOf(c) != -1) {
        return true;
      }
    }
    return false;
  }

  /** Escape special characters in the query string. */
  private String escapeSpecialChars(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    StringBuilder escaped = new StringBuilder();
    for (char c : text.toCharArray()) {
      if (CHARS_TO_ESCAPE.indexOf(c) != -1) {
        escaped.append('\\').append(c);
      } else {
        escaped.append(c);
      }
    }
    return escaped.toString();
  }
}
