/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.exception;

public class SqlParseException extends Exception {

  public SqlParseException(String message) {
    super(message);
  }

  private static final long serialVersionUID = 1L;
}
