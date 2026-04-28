/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Registry of syntax-error suggestion providers, evaluated in priority order. */
public final class SyntaxErrorSuggestionRegistry {
  // CopyOnWriteArrayList: safe iteration during concurrent register() calls.
  private static final CopyOnWriteArrayList<SyntaxErrorSuggestionProvider> PROVIDERS =
      new CopyOnWriteArrayList<>();

  static {
    register(new UnquotedTableNameSuggestionProvider(), new ExpectedTokensSuggestionProvider());
  }

  private SyntaxErrorSuggestionRegistry() {}

  public static void register(SyntaxErrorSuggestionProvider... providers) {
    PROVIDERS.addAll(Arrays.asList(providers));
    PROVIDERS.sort(Comparator.comparingInt(SyntaxErrorSuggestionProvider::getPriority));
  }

  /** Returns suggestions from the first matching provider (by priority); empty list otherwise. */
  public static List<String> findSuggestions(SyntaxErrorContext context) {
    for (SyntaxErrorSuggestionProvider provider : PROVIDERS) {
      List<String> suggestions = provider.getSuggestions(context);
      if (suggestions != null && !suggestions.isEmpty()) {
        return suggestions;
      }
    }
    return List.of();
  }
}
