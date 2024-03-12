/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.exceptions;

/** Exception for taking actions on a disabled datasource. */
public class DatasourceDisabledException extends DataSourceClientException {
  public DatasourceDisabledException(String message) {
    super(message);
  }
}
