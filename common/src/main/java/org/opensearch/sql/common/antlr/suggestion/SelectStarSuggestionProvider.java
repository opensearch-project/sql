/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.Optional;
import java.util.regex.Pattern;

/** Detects SELECT * FROM pattern in PPL context. */
public class SelectStarSuggestionProvider implements SyntaxErrorSuggestionProvider {
  private static final Pattern SELECT_STAR_PATTERN = Pattern.compile("(?i)^\\s*select\\s+\\*.*");
  private static final Pattern SELECT_FIELDS_PATTERN =
      Pattern.compile("(?i)^\\s*select\\s+[a-zA-Z_][a-zA-Z0-9_]*.*");
  // Pattern to extract table name from "SELECT ... FROM table_name" queries
  private static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("(?i).*\\bfrom\\s+([a-zA-Z_][a-zA-Z0-9_\\.]*)", Pattern.DOTALL);

  @Override
  public Optional<String> getSuggestion(SyntaxErrorContext context) {
    // Only suggest PPL syntax when the error originated from the PPL parser.
    if (context.getRecognizer() == null
        || !context.getRecognizer().getGrammarFileName().contains("PPL")) {
      return Optional.empty();
    }

    // Check if query starts with "select" (case-insensitive)
    String query = context.getQuery().trim();
    if (!query.toLowerCase().startsWith("select")) {
      return Optional.empty();
    }

    // Extract table name if available
    String tableName = extractTableName(query);
    String tableRef = tableName != null ? tableName : "index";

    // Check if this looks like SQL SELECT * syntax
    if (SELECT_STAR_PATTERN.matcher(query).matches()) {
      return Optional.of(
          "PPL uses 'source="
              + tableRef
              + " | fields *' instead of 'SELECT * FROM "
              + tableRef
              + "'");
    }

    // Check if this looks like SQL SELECT fields syntax
    if (SELECT_FIELDS_PATTERN.matcher(query).matches()) {
      return Optional.of(
          "PPL uses 'source="
              + tableRef
              + " | fields field1, field2' instead of 'SELECT field1, field2 FROM "
              + tableRef
              + "'");
    }

    return Optional.empty();
  }

  private String extractTableName(String query) {
    java.util.regex.Matcher matcher = TABLE_NAME_PATTERN.matcher(query);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  @Override
  public int getPriority() {
    return 10; // High priority for SQL/PPL syntax confusion
  }
}
