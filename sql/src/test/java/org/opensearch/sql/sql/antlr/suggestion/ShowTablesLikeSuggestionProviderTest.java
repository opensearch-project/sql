/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.suggestion.ShowTablesLikeSuggestionProvider;

class ShowTablesLikeSuggestionProviderTest {
  private final ShowTablesLikeSuggestionProvider provider = new ShowTablesLikeSuggestionProvider();

  @Test
  void suggestsLikeClauseForBareShowTables() {
    List<String> suggestions = provider.getSuggestions(ContextFactory.contextFor("SHOW TABLES"));
    assertEquals(1, suggestions.size());
    assertTrue(suggestions.get(0).contains("Add LIKE clause"));
  }

  @Test
  void suggestsLikeClauseEvenWithTrailingSemicolon() {
    List<String> suggestions = provider.getSuggestions(ContextFactory.contextFor("SHOW TABLES;"));
    assertEquals(1, suggestions.size());
  }

  @Test
  void doesNotMatchOtherQueries() {
    assertTrue(provider.getSuggestions(ContextFactory.contextFor("SELECT FROM t")).isEmpty());
  }
}
