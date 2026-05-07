/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.Optional;

/**
 * Provides syntax error suggestions for a specific pattern. Implementations must be stateless and
 * thread-safe.
 */
public interface SyntaxErrorSuggestionProvider {
  /** Return a suggestion for the given error context, or empty if no suggestion applies. */
  Optional<String> getSuggestion(SyntaxErrorContext context);

  /**
   * Priority for this provider (lower = higher priority). Providers with lower priority values are
   * checked first.
   */
  int getPriority();
}
