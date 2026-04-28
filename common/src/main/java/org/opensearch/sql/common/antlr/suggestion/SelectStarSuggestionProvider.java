/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;

/** Detects SELECT * FROM pattern in PPL context. */
public class SelectStarSuggestionProvider implements SyntaxErrorSuggestionProvider {

  @Override
  public List<String> getSuggestions(SyntaxErrorContext context) {
    if (!"select".equalsIgnoreCase(context.getOffendingText())) {
      return List.of();
    }

    // Check if this looks like SQL syntax in PPL context
    String remaining = context.getRemainingQuery();
    if (remaining.matches("(?i)^\\s*\\*\\s+from\\s+.*")) {
      return List.of(
          "PPL uses 'source=index | fields *' instead of 'SELECT * FROM index'");
    }

    // Also catch other SELECT patterns
    if (remaining.matches("(?i)^\\s+.*\\s+from\\s+.*")) {
      return List.of(
          "PPL uses 'source=index | fields field1, field2' instead of 'SELECT field1, field2 FROM index'");
    }

    return List.of();
  }

  @Override
  public int getPriority() {
    return 10; // High priority for SQL/PPL syntax confusion
  }
}