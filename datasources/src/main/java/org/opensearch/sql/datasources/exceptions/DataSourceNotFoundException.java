/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.exceptions;

/** DataSourceNotFoundException. */
public class DataSourceNotFoundException extends DataSourceClientException {
  public DataSourceNotFoundException(String message) {
    super(message);
  }
}
