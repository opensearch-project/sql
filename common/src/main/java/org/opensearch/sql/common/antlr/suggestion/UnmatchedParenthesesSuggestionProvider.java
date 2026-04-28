/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;

/** Detects unmatched parentheses in queries and suggests balancing them. */
public class UnmatchedParenthesesSuggestionProvider implements SyntaxErrorSuggestionProvider {
  @Override
  public List<String> getSuggestions(SyntaxErrorContext ctx) {
    String query = ctx.getQuery();
    if (query == null || query.isEmpty()) return List.of();

    // Check if we have unmatched parentheses
    int openParens = 0;
    int closeParens = 0;

    for (char c : query.toCharArray()) {
      if (c == '(') {
        openParens++;
      } else if (c == ')') {
        closeParens++;
      }
    }

    // If we have more opening than closing parentheses
    if (openParens > closeParens) {
      int missing = openParens - closeParens;
      if (missing == 1) {
        return List.of("Missing closing parenthesis ')'. Check that all parentheses are balanced");
      } else {
        return List.of(String.format("Missing %d closing parentheses ')'. Check that all parentheses are balanced", missing));
      }
    }

    // If we have more closing than opening parentheses
    if (closeParens > openParens) {
      int extra = closeParens - openParens;
      if (extra == 1) {
        return List.of("Extra closing parenthesis ')'. Check that all parentheses are balanced");
      } else {
        return List.of(String.format("Extra %d closing parentheses ')'. Check that all parentheses are balanced", extra));
      }
    }

    return List.of();
  }

  @Override
  public int getPriority() {
    return 20; // High priority since unmatched parentheses are common
  }
}