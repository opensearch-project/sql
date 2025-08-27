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
import org.opensearch.sql.utils.QueryStringUtils;

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
  private final SearchLiteral value;

  @Override
  public String toQueryString() {
    String fieldName = QueryStringUtils.escapeFieldName(field.getField().toString());
    String valueStr = value.toQueryString();
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

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Arrays.asList(field, value);
  }
}
