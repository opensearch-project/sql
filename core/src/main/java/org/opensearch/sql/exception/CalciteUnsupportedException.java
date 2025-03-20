/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

public class CalciteUnsupportedException extends QueryEngineException {

  public CalciteUnsupportedException(String message) {
    super(message);
  }
}
