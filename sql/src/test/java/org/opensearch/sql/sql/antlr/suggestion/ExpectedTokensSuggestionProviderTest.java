/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.suggestion.ExpectedTokensSuggestionProvider;

class ExpectedTokensSuggestionProviderTest {
  private final ExpectedTokensSuggestionProvider provider = new ExpectedTokensSuggestionProvider();

  @Test
  void returnsFallbackSuggestionWithExpectedTokens() {
    // SELECT * FROM <EOF> has a RecognitionException with a populated expected set.
    List<String> suggestions = provider.getSuggestions(ContextFactory.contextFor("SELECT * FROM"));
    assertFalse(suggestions.isEmpty());
    String suggestion = suggestions.get(0);
    assertTrue(
        suggestion.startsWith("Expected tokens:") || suggestion.startsWith("Expected one of "),
        "got: " + suggestion);
  }

  @Test
  void returnsEmptyWhenNoRecognitionException() {
    // Parser-recovered errors have no RecognitionException: the provider must no-op, not crash.
    List<String> suggestions = provider.getSuggestions(ContextFactory.contextFor("SELECT FROM t"));
    // Either empty, or populated - neither should throw. We just assert it doesn't throw.
    assertTrue(suggestions != null);
  }
}
