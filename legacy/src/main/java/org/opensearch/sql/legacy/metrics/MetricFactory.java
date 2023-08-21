/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;

public class MetricFactory {

  public static Metric createMetric(MetricName name) {

    switch (name) {
      case REQ_TOTAL:
      case DEFAULT_CURSOR_REQUEST_TOTAL:
      case DEFAULT:
      case PPL_REQ_TOTAL:
        return new NumericMetric<>(name.getName(), new BasicCounter());
      case CIRCUIT_BREAKER:
        return new GaugeMetric<>(name.getName(), BackOffRetryStrategy.GET_CB_STATE);
      case REQ_COUNT_TOTAL:
      case DEFAULT_CURSOR_REQUEST_COUNT_TOTAL:
      case FAILED_REQ_COUNT_CUS:
      case FAILED_REQ_COUNT_SYS:
      case FAILED_REQ_COUNT_CB:
      case PPL_REQ_COUNT_TOTAL:
      case PPL_FAILED_REQ_COUNT_CUS:
      case PPL_FAILED_REQ_COUNT_SYS:
        return new NumericMetric<>(name.getName(), new RollingCounter());
      default:
        return new NumericMetric<>(name.getName(), new BasicCounter());
    }
  }
}
