/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

/** Non-fallback to v2 exception for Calcite. */
public class NonFallbackCalciteException extends QueryEngineException {

  public NonFallbackCalciteException(String message) {
    super(message);
  }
}
