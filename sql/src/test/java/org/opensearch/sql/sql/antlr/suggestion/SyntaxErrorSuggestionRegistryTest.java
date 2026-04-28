/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.suggestion.SyntaxErrorContext;
import org.opensearch.sql.common.antlr.suggestion.SyntaxErrorSuggestionProvider;
import org.opensearch.sql.common.antlr.suggestion.SyntaxErrorSuggestionRegistry;

class SyntaxErrorSuggestionRegistryTest {

  /** Lower priority value must win over a higher-priority-number provider on the same match. */
  @Test
  void lowerPriorityProviderWinsOverHigherPriorityProvider() {
    StubProvider lowPrioritySuggestion = new StubProvider("low-wins", 1);
    StubProvider highPrioritySuggestion = new StubProvider("high-loses", Integer.MAX_VALUE - 1);
    SyntaxErrorSuggestionRegistry.register(highPrioritySuggestion, lowPrioritySuggestion);

    // Provide a context that both will match (both stubs ignore the context).
    SyntaxErrorContext ctx = ContextFactory.contextFor("SELECT FROM t");
    List<String> suggestions = SyntaxErrorSuggestionRegistry.findSuggestions(ctx);

    assertEquals("low-wins", suggestions.get(0));
  }

  private static class StubProvider implements SyntaxErrorSuggestionProvider {
    private final String suggestion;
    private final int priority;

    StubProvider(String suggestion, int priority) {
      this.suggestion = suggestion;
      this.priority = priority;
    }

    @Override
    public List<String> getSuggestions(SyntaxErrorContext context) {
      return List.of(suggestion);
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }
}
