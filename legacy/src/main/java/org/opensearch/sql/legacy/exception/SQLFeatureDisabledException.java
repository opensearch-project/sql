/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.exception;

public class SQLFeatureDisabledException extends Exception {

  private static final long serialVersionUID = 1L;

  public SQLFeatureDisabledException(String message) {
    super(message);
  }
}
