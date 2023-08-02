/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

/** Query analysis abstract exception. */
public class QueryEngineException extends RuntimeException {

  public QueryEngineException(String message) {
    super(message);
  }
}
