/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr;

/** SQL query analysis abstract exception. */
public class SqlAnalysisException extends RuntimeException {

  public SqlAnalysisException(String message) {
    super(message);
  }
}
