/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.metrics;

public interface MetricsService {
  void incrementNumericalMetric(EmrMetrics emrMetrics);
}
