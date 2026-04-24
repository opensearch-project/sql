/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;
import org.antlr.v4.runtime.Token;

/** Detects unquoted table identifiers containing special chars (e.g. {@code hello+world}). */
public class UnquotedTableNameSuggestionProvider implements SyntaxErrorSuggestionProvider {
  @Override
  public List<String> getSuggestions(SyntaxErrorContext ctx) {
    String offending = ctx.getOffendingText();
    if (offending == null || offending.isEmpty()) return List.of();
    // Offending text is a special/punctuation char appearing after a FROM identifier.
    if (!offending.matches("[^A-Za-z0-9_`'\"\\s.,()*]")) return List.of();
    if (!followsFromClause(ctx)) return List.of();
    return List.of(
        "Quote table names containing special characters with backticks, e.g. `hello+world`");
  }

  @Override
  public int getPriority() {
    return 30;
  }

  private static boolean followsFromClause(SyntaxErrorContext ctx) {
    Token offending = ctx.getOffendingToken();
    if (offending == null) return false;
    List<Token> all = ctx.getAllTokens();
    for (int i = offending.getTokenIndex() - 1; i >= 0; i--) {
      Token t = all.get(i);
      if (t.getChannel() != Token.DEFAULT_CHANNEL) continue;
      if ("from".equalsIgnoreCase(t.getText())) return true;
    }
    return false;
  }
}
