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
 * Translates search expressions containing free text and boolean operators into query_string
 * function calls. Handles boolean operators (AND, OR, NOT) natively supported by query_string.
 */
public class SearchTextTranslator {

  // Reserved characters in query_string that need escaping when in literal text
  // We don't escape: ( ) for grouping, " for phrases, * ? for wildcards, AND OR NOT operators
  // We DO escape: + - = && || > < ! { } [ ] ^ ~ : \ /
  private static final String CHARS_TO_ESCAPE = "+-=><![]{^~:\\/";

  /** Translate search expressions into query_string function calls where possible. */
  public UnresolvedExpression translate(List<UnresolvedExpression> expressions) {
    if (expressions.isEmpty()) {
      return null;
    }

    if (expressions.size() == 1) {
      return processExpression(expressions.getFirst());
    }

    // Build expression tree first with implicit AND between expressions
    UnresolvedExpression combined =
        expressions.stream().reduce((left, right) -> AstDSL.and(left, right)).get();

    // Now optimize the combined tree
    return processExpression(combined);
  }

  /** Process a single expression, potentially converting it to query_string. */
  private UnresolvedExpression processExpression(UnresolvedExpression expr) {
    // Check if entire expression tree contains only search text
    if (isEntirelySearchText(expr)) {
      // Convert the entire expression to a query_string
      String queryString = buildQueryString(expr);
      return createQueryString(queryString);
    }

    // Otherwise, process based on type
    if (expr instanceof And) {
      return processAndExpression((And) expr);
    } else if (expr instanceof Or) {
      return processOrExpression((Or) expr);
    } else if (expr instanceof Not) {
      return processNotExpression((Not) expr);
    } else if (isSearchText(expr)) {
      // Single search text term
      return createQueryString(convertToQueryString(expr));
    } else {
      // Regular filter expression (comparison, function, etc.)
      return expr;
    }
  }

  /** Check if an entire expression tree contains only search text (no field comparisons). */
  private boolean isEntirelySearchText(UnresolvedExpression expr) {
    if (expr instanceof And) {
      And and = (And) expr;
      return isEntirelySearchText(and.getLeft()) && isEntirelySearchText(and.getRight());
    } else if (expr instanceof Or) {
      Or or = (Or) expr;
      return isEntirelySearchText(or.getLeft()) && isEntirelySearchText(or.getRight());
    } else if (expr instanceof Not) {
      Not not = (Not) expr;
      return isEntirelySearchText(not.getExpression());
    } else {
      return isSearchText(expr);
    }
  }

  /** Build a query string from an expression tree. */
  private String buildQueryString(UnresolvedExpression expr) {
    if (expr instanceof And) {
      And and = (And) expr;
      String left = buildQueryString(and.getLeft());
      String right = buildQueryString(and.getRight());
      // Wrap in parentheses to preserve precedence
      return "(" + left + " AND " + right + ")";
    } else if (expr instanceof Or) {
      Or or = (Or) expr;
      String left = buildQueryString(or.getLeft());
      String right = buildQueryString(or.getRight());
      // Wrap in parentheses to preserve precedence
      return "(" + left + " OR " + right + ")";
    } else if (expr instanceof Not) {
      Not not = (Not) expr;
      String inner = buildQueryString(not.getExpression());
      // NOT already has parentheses around its operand
      return "NOT (" + inner + ")";
    } else if (isSearchText(expr)) {
      return convertToQueryString(expr);
    } else {
      // Shouldn't happen if isEntirelySearchText is correct
      return expr.toString();
    }
  }

  /** Process AND expression when it contains mixed content (search text and filters). */
  private UnresolvedExpression processAndExpression(And andExpr) {
    // Process left and right sides
    UnresolvedExpression left = processExpression(andExpr.getLeft());
    UnresolvedExpression right = processExpression(andExpr.getRight());

    // Combine the results
    if (left == null && right == null) {
      return null;
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    } else {
      return AstDSL.and(left, right);
    }
  }

  /** Process OR expression when it contains mixed content (search text and filters). */
  private UnresolvedExpression processOrExpression(Or orExpr) {
    // Process left and right sides
    UnresolvedExpression left = processExpression(orExpr.getLeft());
    UnresolvedExpression right = processExpression(orExpr.getRight());

    // Combine the results
    if (left == null && right == null) {
      return null;
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    } else {
      return AstDSL.or(left, right);
    }
  }

  /** Process NOT expression when it contains mixed content. */
  private UnresolvedExpression processNotExpression(Not notExpr) {
    // Process the inner expression
    UnresolvedExpression inner = processExpression(notExpr.getExpression());

    if (inner == null) {
      return null;
    } else {
      return AstDSL.not(inner);
    }
  }

  /** Check if an expression should be treated as search text. */
  private boolean isSearchText(UnresolvedExpression expr) {
    if (expr instanceof SearchMarkerExpression) {
      // SearchMarkerExpression explicitly marks search text
      return true;
    } else if (expr instanceof Field) {
      // In some cases, Field objects may not be wrapped in SearchMarkerExpression
      // This can happen with nested boolean expressions
      Field field = (Field) expr;
      UnresolvedExpression innerExpr = field.getField();
      if (innerExpr instanceof QualifiedName) {
        QualifiedName qName = (QualifiedName) innerExpr;
        // Single-part names are search text, multi-part are field references
        return qName.getParts().size() == 1;
      }
      return false;
    } else if (expr instanceof Function) {
      // Check if it's already a query_string function
      Function func = (Function) expr;
      return "query_string".equals(func.getFuncName());
    }
    // All other expressions are not search text
    // (comparisons, field references, etc.)
    return false;
  }

  /** Convert an expression to query_string syntax. */
  private String convertToQueryString(UnresolvedExpression expr) {
    if (expr instanceof SearchMarkerExpression) {
      // Unwrap the SearchMarkerExpression and process its inner expression
      SearchMarkerExpression marker = (SearchMarkerExpression) expr;
      UnresolvedExpression inner = marker.getExpression();

      // Handle different types of inner expressions
      if (inner instanceof Literal) {
        Literal literal = (Literal) inner;
        Object value = literal.getValue();
        if (value instanceof String) {
          String strValue = (String) value;
          // The parser has already removed outer quotes from string literals
          // We need to determine if this was originally a quoted phrase
          // Heuristic: if it contains spaces or special chars that would normally break a word, it
          // was quoted
          if (strValue.contains(" ") || containsUnescapedSpecialChars(strValue)) {
            // This was likely a quoted phrase - escape content and wrap in quotes
            String escaped = escapeReservedChars(strValue);
            return "\"" + escaped.replace("\"", "\\\"") + "\"";
          } else {
            // Simple word - escape reserved characters but keep wildcards
            return escapeReservedCharsKeepWildcards(strValue);
          }
        } else {
          // Numeric literals - just convert to string
          return value.toString();
        }
      }
      // Fallback for any other type wrapped in SearchMarker
      return inner.toString();
    } else if (expr instanceof Field) {
      // Handle Field objects that may not be wrapped in SearchMarkerExpression
      Field field = (Field) expr;
      UnresolvedExpression fieldExpr = field.getField();
      if (fieldExpr instanceof QualifiedName) {
        String fieldName = ((QualifiedName) fieldExpr).toString();
        return escapeReservedCharsKeepWildcards(fieldName);
      }
      return fieldExpr.toString();
    } else if (expr instanceof Function) {
      // If it's already a query_string, extract its query
      Function func = (Function) expr;
      if ("query_string".equals(func.getFuncName())) {
        return extractQueryString(expr);
      }
      return expr.toString();
    }

    // For any other expression type, just return its string representation
    return expr.toString();
  }

  /**
   * Escape reserved characters in search text, but preserve wildcards. Escapes: + - = > < ! { } [ ]
   * ^ ~ : \ / Preserves: * ? (wildcards) and parentheses (handled at expression level)
   */
  private String escapeReservedCharsKeepWildcards(String text) {
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

  /**
   * Escape reserved characters in quoted phrase content. Inside quotes, we escape everything except
   * the quote itself (handled separately).
   */
  private String escapeReservedChars(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    StringBuilder escaped = new StringBuilder();
    for (char c : text.toCharArray()) {
      // Inside quotes, escape these reserved chars
      if (CHARS_TO_ESCAPE.indexOf(c) != -1) {
        escaped.append('\\').append(c);
      } else {
        escaped.append(c);
      }
    }
    return escaped.toString();
  }

  /**
   * Check if string contains special characters that would require quoting. Used to determine if a
   * literal was originally quoted.
   */
  private boolean containsUnescapedSpecialChars(String text) {
    // Check for chars that would break word boundaries in query_string
    // This includes operators and special syntax chars
    String specialChars = "+=<>!(){}[]^~:\\/";
    for (char c : text.toCharArray()) {
      if (specialChars.indexOf(c) != -1) {
        return true;
      }
    }
    return false;
  }

  /** Create a query_string function call. */
  private UnresolvedExpression createQueryString(String queryString) {
    // Create the function with query parameter and default_operator=AND
    List<UnresolvedExpression> args = new ArrayList<>();
    args.add(AstDSL.unresolvedArg("query", AstDSL.stringLiteral(queryString)));
    args.add(AstDSL.unresolvedArg("default_operator", AstDSL.stringLiteral("AND")));

    return AstDSL.function("query_string", args.toArray(new UnresolvedExpression[0]));
  }

  /** Extract the query string from a query_string function. */
  private String extractQueryString(UnresolvedExpression expr) {
    if (expr instanceof Function) {
      Function func = (Function) expr;
      if ("query_string".equals(func.getFuncName()) && !func.getFuncArgs().isEmpty()) {
        UnresolvedExpression firstArg = func.getFuncArgs().get(0);
        if (firstArg instanceof UnresolvedArgument) {
          UnresolvedArgument arg = (UnresolvedArgument) firstArg;
          if (arg.getValue() instanceof Literal) {
            return ((Literal) arg.getValue()).getValue().toString();
          }
        }
      }
    }
    return "";
  }
}
