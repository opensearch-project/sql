/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.suggestion.UnquotedTableNameSuggestionProvider;

class UnquotedTableNameSuggestionProviderTest {
  private final UnquotedTableNameSuggestionProvider provider =
      new UnquotedTableNameSuggestionProvider();

  @Test
  void suggestsBacktickQuotingForSpecialCharInTableName() {
    List<String> suggestions =
        provider.getSuggestions(ContextFactory.contextFor("SELECT * FROM hello+world"));
    assertEquals(1, suggestions.size());
    assertTrue(suggestions.get(0).contains("Quote table names"));
    assertTrue(suggestions.get(0).contains("`hello+world`"));
  }

  @Test
  void doesNotMatchSpecialCharsOutsideFromClause() {
    // SELECT 1 +  - offending char is after SELECT, not FROM context
    assertTrue(provider.getSuggestions(ContextFactory.contextFor("SELECT 1 + FROM t")).isEmpty());
  }

  @Test
  void doesNotMatchEofOffendingToken() {
    // SELECT * FROM - no trailing special char
    assertTrue(provider.getSuggestions(ContextFactory.contextFor("SELECT * FROM")).isEmpty());
  }
}
