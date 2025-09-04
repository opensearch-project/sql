/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client.exceptions;

/** Exception thrown when there are issues with data source client operations. */
public class DataSourceClientException extends RuntimeException {

  public DataSourceClientException(String message) {
    super(message);
  }

  public DataSourceClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
