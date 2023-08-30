/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.exceptions;

/**
 * This is the base class for all DataSource Client Exceptions. All datasource connector modules
 * will extend this exception for their respective connection clients.
 */
public class DataSourceClientException extends RuntimeException {

  public DataSourceClientException(String message) {
    super(message);
  }
}
