/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

/** Semantic Check Exception. */
public class SemanticCheckException extends QueryEngineException {
  public SemanticCheckException(String message) {
    super(message);
  }
}
