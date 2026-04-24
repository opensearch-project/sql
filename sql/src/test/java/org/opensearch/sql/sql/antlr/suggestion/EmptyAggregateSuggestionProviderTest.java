/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr.suggestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.common.antlr.suggestion.EmptyAggregateSuggestionProvider;

class EmptyAggregateSuggestionProviderTest {
  private final EmptyAggregateSuggestionProvider provider = new EmptyAggregateSuggestionProvider();

  @ParameterizedTest
  @ValueSource(strings = {"SUM", "AVG", "MIN", "MAX"})
  void suggestsColumnNameForEmptyAggregate(String agg) {
    List<String> suggestions =
        provider.getSuggestions(ContextFactory.contextFor("SELECT " + agg + "() FROM t"));
    assertEquals(1, suggestions.size());
    assertTrue(suggestions.get(0).contains("Add a column name"));
    assertTrue(suggestions.get(0).contains(agg + "(column_name)"));
  }

  @Test
  void suggestsCountStarForEmptyCount() {
    List<String> suggestions =
        provider.getSuggestions(ContextFactory.contextFor("SELECT COUNT() FROM t"));
    assertEquals(2, suggestions.size());
    assertTrue(suggestions.stream().anyMatch(s -> s.contains("Add a column name")));
    assertTrue(suggestions.stream().anyMatch(s -> s.contains("COUNT(*)")));
  }

  @Test
  void doesNotMatchWhenOffendingTokenIsNotCloseParen() {
    // SELECT FROM t - offending token is FROM, not )
    assertTrue(provider.getSuggestions(ContextFactory.contextFor("SELECT FROM t")).isEmpty());
  }

  @Test
  void doesNotMatchNonAggregateFunctions() {
    // Emitting an unmatched ) alone doesn't follow an aggregate function pattern.
    assertTrue(provider.getSuggestions(ContextFactory.contextFor("SELECT ) FROM t")).isEmpty());
  }
}
