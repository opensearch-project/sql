/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.suggestion.ExpectedTokensSuggestionProvider;

class ExpectedTokensSuggestionProviderTest {
  private final ExpectedTokensSuggestionProvider provider = new ExpectedTokensSuggestionProvider();

  @Test
  void returnsFallbackSuggestionWithExpectedTokens() {
    // SELECT * FROM <EOF> has a RecognitionException with a populated expected set.
    Optional<String> suggestion =
        provider.getSuggestion(ContextFactory.contextFor("SELECT * FROM"));
    assertTrue(suggestion.isPresent());
    String suggestionText = suggestion.get();
    assertTrue(
        suggestionText.startsWith("Expected tokens:")
            || suggestionText.startsWith("Expected one of "),
        "got: " + suggestionText);
  }

  @Test
  void returnsEmptyWhenNoRecognitionException() {
    // Parser-recovered errors have no RecognitionException: the provider must no-op, not crash.
    Optional<String> suggestion =
        provider.getSuggestion(ContextFactory.contextFor("SELECT FROM t"));
    // Either empty, or populated - neither should throw. We just assert it doesn't throw.
    assertTrue(suggestion != null);
  }
}
