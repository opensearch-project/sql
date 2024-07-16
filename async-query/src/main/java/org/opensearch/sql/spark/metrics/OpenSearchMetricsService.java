/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.metrics;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.utils.MetricUtils;

public class OpenSearchMetricsService implements MetricsService {
  private static final Map<EmrMetrics, MetricName> mapping =
      ImmutableMap.of(
          EmrMetrics.EMR_CANCEL_JOB_REQUEST_FAILURE_COUNT,
              MetricName.EMR_CANCEL_JOB_REQUEST_FAILURE_COUNT,
          EmrMetrics.EMR_GET_JOB_RESULT_FAILURE_COUNT, MetricName.EMR_GET_JOB_RESULT_FAILURE_COUNT,
          EmrMetrics.EMR_START_JOB_REQUEST_FAILURE_COUNT,
              MetricName.EMR_START_JOB_REQUEST_FAILURE_COUNT,
          EmrMetrics.EMR_INTERACTIVE_QUERY_JOBS_CREATION_COUNT,
              MetricName.EMR_INTERACTIVE_QUERY_JOBS_CREATION_COUNT,
          EmrMetrics.EMR_STREAMING_QUERY_JOBS_CREATION_COUNT,
              MetricName.EMR_STREAMING_QUERY_JOBS_CREATION_COUNT,
          EmrMetrics.EMR_BATCH_QUERY_JOBS_CREATION_COUNT,
              MetricName.EMR_BATCH_QUERY_JOBS_CREATION_COUNT);

  @Override
  public void incrementNumericalMetric(EmrMetrics metricName) {
    MetricUtils.incrementNumericalMetric(mapping.get(metricName));
  }
}
