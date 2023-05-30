/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.BACKTICK_QUOTE_ID;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.BOOLEAN;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.COMMA;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.DECIMAL_LITERAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.DOT;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.EQUAL_SYMBOL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.EXCLAMATION_SYMBOL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.FALSE;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.FROM;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.GREATER_SYMBOL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.ID;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.LESS_SYMBOL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.ONE_DECIMAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.REAL_LITERAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.STRING_LITERAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.TIMESTAMP;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.TRUE;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.TWO_DECIMAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.ZERO_DECIMAL;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Parse tree listener for anonymizing SQL requests.
 */
public class AnonymizerListener implements ParseTreeListener {
  private String anonymizedQueryString = "";
  private static final int NO_TYPE = -1;
  private int previousType = NO_TYPE;

  @Override
  public void enterEveryRule(ParserRuleContext ctx) {
  }

  @Override
  public void exitEveryRule(ParserRuleContext ctx) {
  }

  @Override
  public void visitTerminal(TerminalNode node) {
    // In these situations don't add a space prior:
    // 1. a DOT between two identifiers
    // 2. before a comma
    // 3. between equal comparison tokens: e.g <=
    // 4.  between alt not equals: <>
    int token = node.getSymbol().getType();
    boolean isDotIdentifiers = token == DOT || previousType == DOT;
    boolean isComma = token == COMMA;
    boolean isEqualComparison = ((token == EQUAL_SYMBOL)
            && (previousType == LESS_SYMBOL
        || previousType == GREATER_SYMBOL
        || previousType == EXCLAMATION_SYMBOL));
    boolean isNotEqualComparisonAlternative =
        previousType == LESS_SYMBOL && token == GREATER_SYMBOL;
    if (!isDotIdentifiers && !isComma && !isEqualComparison && !isNotEqualComparisonAlternative) {
      anonymizedQueryString += " ";
    }

    // anonymize the following tokens
    switch (node.getSymbol().getType()) {
      case ID:
      case TIMESTAMP:
      case BACKTICK_QUOTE_ID:
        if (previousType == FROM) {
          anonymizedQueryString += "table";
        } else {
          anonymizedQueryString += "identifier";
        }
        break;
      case ZERO_DECIMAL:
      case ONE_DECIMAL:
      case TWO_DECIMAL:
      case DECIMAL_LITERAL:
      case REAL_LITERAL:
        anonymizedQueryString += "number";
        break;
      case STRING_LITERAL:
        anonymizedQueryString += "'string_literal'";
        break;
      case BOOLEAN:
      case TRUE:
      case FALSE:
        anonymizedQueryString += "boolean_literal";
        break;
      case NO_TYPE:
        // end of file
        break;
      default:
        anonymizedQueryString += node.getText().toUpperCase();
    }
    previousType = node.getSymbol().getType();
  }

  @Override
  public void visitErrorNode(ErrorNode node) {

  }

  public String getAnonymizedQueryString() {
    return "(" + anonymizedQueryString + ")";
  }
}
