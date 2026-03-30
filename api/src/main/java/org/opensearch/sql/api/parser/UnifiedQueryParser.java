/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

/**
 * Language-neutral query parser interface. Returns the native parse result for the language (e.g.,
 * {@code UnresolvedPlan} for PPL, {@code SqlNode} for Calcite SQL).
 *
 * @param <T> the native parse result type for this language
 */
public interface UnifiedQueryParser<T> {

  /**
   * Parses the query and returns the native parse result.
   *
   * @param query the raw query string
   * @return the native parse result
   */
  T parse(String query);
}
