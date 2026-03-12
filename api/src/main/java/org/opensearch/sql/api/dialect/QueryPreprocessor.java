/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

/**
 * Per-dialect preprocessor that transforms raw query strings before they reach the Calcite SQL
 * parser. Implementations strip or transform dialect-specific clauses that Calcite cannot parse.
 */
public interface QueryPreprocessor {

  /**
   * Preprocess the raw query string, stripping or transforming dialect-specific clauses.
   *
   * @param query the raw query string
   * @return the cleaned query string ready for Calcite parsing
   */
  String preprocess(String query);
}
