/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.antlr.v4.runtime.Token;

/** Detects empty aggregate calls like {@code SUM()}, {@code COUNT()} and suggests fixes. */
public class EmptyAggregateSuggestionProvider implements SyntaxErrorSuggestionProvider {
  private static final Set<String> AGGREGATES = Set.of("sum", "avg", "min", "max", "count");

  @Override
  public List<String> getSuggestions(SyntaxErrorContext ctx) {
    if (!")".equals(ctx.getOffendingText())) return List.of();
    Token prev = previousNonHidden(ctx);
    if (prev == null || !"(".equals(prev.getText())) return List.of();
    Token fn = previousNonHidden(ctx, prev.getTokenIndex());
    if (fn == null) return List.of();
    String name = fn.getText().toLowerCase();
    if (!AGGREGATES.contains(name)) return List.of();

    String upper = name.toUpperCase();
    List<String> out = new ArrayList<>();
    out.add(String.format("Add a column name inside %s(): e.g. %s(column_name)", upper, upper));
    if ("count".equals(name)) {
      out.add("Use COUNT(*) to count all rows");
    }
    return out;
  }

  @Override
  public int getPriority() {
    return 20;
  }

  private static Token previousNonHidden(SyntaxErrorContext ctx) {
    Token offending = ctx.getOffendingToken();
    return offending == null ? null : previousNonHidden(ctx, offending.getTokenIndex());
  }

  private static Token previousNonHidden(SyntaxErrorContext ctx, int fromIndex) {
    List<Token> all = ctx.getAllTokens();
    for (int i = fromIndex - 1; i >= 0; i--) {
      Token t = all.get(i);
      if (t.getChannel() == Token.DEFAULT_CHANNEL && !t.getText().trim().isEmpty()) return t;
    }
    return null;
  }
}
