/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/** Search expression for field comparisons. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchComparison extends SearchExpression {

  public enum Operator {
    EQUALS("="),
    NOT_EQUALS("!="),
    LESS_THAN("<"),
    LESS_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_OR_EQUAL(">=");

    private final String symbol;

    Operator(String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }
  }

  private final Field field;
  private final Operator operator;
  private final UnresolvedExpression value;

  @Override
  public String toQueryString() {
    String fieldName = QueryStringUtils.escapeFieldName(field.getField().toString());
    String valueStr = formatValue(value);
    return switch (operator) {
      case NOT_EQUALS -> "( _exists_:"
          + fieldName
          + " AND NOT "
          + fieldName
          + ":"
          + valueStr
          + " )";
      case GREATER_THAN -> fieldName + ":>" + valueStr;
      case GREATER_OR_EQUAL -> fieldName + ":>=" + valueStr;
      case LESS_THAN -> fieldName + ":<" + valueStr;
      case LESS_OR_EQUAL -> fieldName + ":<=" + valueStr;
      default -> fieldName + ":" + valueStr;
    };
  }

  private String formatValue(UnresolvedExpression expr) {
    if (expr instanceof Literal) {
      Literal lit = (Literal) expr;
      Object val = lit.getValue();

      // Numbers don't need escaping
      if (val instanceof Number) {
        return val.toString();
      }

      // Booleans don't need escaping
      if (val instanceof Boolean) {
        return val.toString();
      }

      // Strings
      if (val instanceof String) {
        String str = (String) val;

        // Check if this is a numeric literal with suffix (like "0.1d" or "0.1f")
        // These should be passed through as-is without escaping
        if (str.matches("-?\\d+(\\.\\d+)?[dDfF]?")) {
          return str;
        }

        // Regular strings need Lucene escaping
        str = QueryStringUtils.escapeLuceneSpecialCharacters(str);
        // Add quotes if contains spaces
        if (str.contains(" ")) {
          return "\"" + str + "\"";
        }
        return str;
      }
    }

    // For other types, get the text representation
    return QueryStringUtils.escapeLuceneSpecialCharacters(expr.toString());
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Arrays.asList(field, value);
  }
}
