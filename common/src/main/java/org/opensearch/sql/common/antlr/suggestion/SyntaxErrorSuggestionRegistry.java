/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/** Registry of syntax-error suggestion providers, evaluated in priority order. */
public final class SyntaxErrorSuggestionRegistry {
  // CopyOnWriteArrayList: safe iteration during concurrent register() calls.
  private static final CopyOnWriteArrayList<SyntaxErrorSuggestionProvider> PROVIDERS =
      new CopyOnWriteArrayList<>();

  static {
    register(
        new SelectStarSuggestionProvider(),
        new UnmatchedParenthesesSuggestionProvider(),
        new ExpectedTokensSuggestionProvider());
  }

  private SyntaxErrorSuggestionRegistry() {}

  public static void register(SyntaxErrorSuggestionProvider... providers) {
    PROVIDERS.addAll(Arrays.asList(providers));
    PROVIDERS.sort(Comparator.comparingInt(SyntaxErrorSuggestionProvider::getPriority));
  }

  /** Returns suggestion from the first matching provider (by priority); empty otherwise. */
  public static Optional<String> findSuggestion(SyntaxErrorContext context) {
    for (SyntaxErrorSuggestionProvider provider : PROVIDERS) {
      Optional<String> suggestion = provider.getSuggestion(context);
      if (suggestion.isPresent()) {
        return suggestion;
      }
    }
    return Optional.empty();
  }
}
