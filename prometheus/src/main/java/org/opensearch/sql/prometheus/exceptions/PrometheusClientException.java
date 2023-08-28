/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.exceptions;

import org.opensearch.sql.datasources.exceptions.DataSourceClientException;

/** PrometheusClientException. */
public class PrometheusClientException extends DataSourceClientException {
  public PrometheusClientException(String message) {
    super(message);
  }
}
