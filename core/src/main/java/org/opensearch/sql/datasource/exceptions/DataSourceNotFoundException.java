/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasource.exceptions;

/**
 * DataSourceNotFoundException.
 */
public class DataSourceNotFoundException extends RuntimeException {
  public DataSourceNotFoundException(String message) {
    super(message);
  }

}
