/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

/** Non-fallback to v2 exception for Calcite. */
public class NonFallbackCalciteException extends RuntimeException {

  public NonFallbackCalciteException(String message) {
    super(message);
  }

  public NonFallbackCalciteException(String message, Throwable cause) {
    super(message, cause);
  }
}
