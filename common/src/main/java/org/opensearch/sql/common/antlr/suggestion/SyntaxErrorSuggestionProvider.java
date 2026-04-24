/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.List;

/**
 * Provides syntax error suggestions for a specific pattern. Implementations must be stateless and
 * thread-safe.
 */
public interface SyntaxErrorSuggestionProvider {
  /** Return zero or more suggestions for the given error context. */
  List<String> getSuggestions(SyntaxErrorContext context);

  /** Lower = checked earlier. Ties are kept in registration order. */
  int getPriority();
}
