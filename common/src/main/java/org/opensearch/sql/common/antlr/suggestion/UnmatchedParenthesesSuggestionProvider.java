/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;
import org.antlr.v4.runtime.Token;

/** Detects unmatched parentheses in queries and suggests balancing them. */
public class UnmatchedParenthesesSuggestionProvider implements SyntaxErrorSuggestionProvider {
  @Override
  public List<String> getSuggestions(SyntaxErrorContext ctx) {
    if (ctx.getQuery() == null || ctx.getQuery().isEmpty()) return List.of();

    // Count paren tokens on the default channel, which skips string literals, comments,
    // and whitespace so we don't get false positives from e.g. `where msg = "ignore )"`.
    int openParens = 0;
    int closeParens = 0;
    for (Token t : ctx.getAllTokens()) {
      if (t.getChannel() != Token.DEFAULT_CHANNEL) continue;
      String text = t.getText();
      if ("(".equals(text)) {
        openParens++;
      } else if (")".equals(text)) {
        closeParens++;
      }
    }

    // If we have more opening than closing parentheses
    if (openParens > closeParens) {
      int missing = openParens - closeParens;
      if (missing == 1) {
        return List.of("Missing closing parenthesis ')'. Check that all parentheses are balanced");
      } else {
        return List.of(
            String.format(
                "Missing %d closing parentheses ')'. Check that all parentheses are balanced",
                missing));
      }
    }

    // If we have more closing than opening parentheses
    if (closeParens > openParens) {
      int extra = closeParens - openParens;
      if (extra == 1) {
        return List.of("Extra closing parenthesis ')'. Check that all parentheses are balanced");
      } else {
        return List.of(
            String.format(
                "Extra %d closing parentheses ')'. Check that all parentheses are balanced",
                extra));
      }
    }

    return List.of();
  }

  @Override
  public int getPriority() {
    return 20; // High priority since unmatched parentheses are common
  }
}
