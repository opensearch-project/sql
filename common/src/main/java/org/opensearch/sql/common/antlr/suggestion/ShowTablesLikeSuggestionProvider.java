/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;

/** Detects {@code SHOW TABLES} without a {@code LIKE} clause. */
public class ShowTablesLikeSuggestionProvider implements SyntaxErrorSuggestionProvider {
  @Override
  public List<String> getSuggestions(SyntaxErrorContext ctx) {
    String q = ctx.getQuery();
    if (q == null) return List.of();
    if (!q.trim().toLowerCase().matches("show\\s+tables\\s*;?")) return List.of();
    return List.of("Add LIKE clause with a quoted pattern, e.g. SHOW TABLES LIKE '%'");
  }

  @Override
  public int getPriority() {
    return 15;
  }
}
