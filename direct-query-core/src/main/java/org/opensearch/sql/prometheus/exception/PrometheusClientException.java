/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.exception;

import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;

/** PrometheusClientException. */
public class PrometheusClientException extends DataSourceClientException {
  public PrometheusClientException(String message) {
    super(message);
  }
}
