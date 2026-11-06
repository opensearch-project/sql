/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

/**
 * Query languages with latency stats. Kept separate from {@link MetricName} so a latency metric
 * can't be passed to {@link Metrics#getNumericalMetric} and incremented like a counter.
 */
public enum LatencyMetricName {
  SQL("sql"),
  PPL("ppl");

  private final String prefix;

  LatencyMetricName(String prefix) {
    this.prefix = prefix;
  }

  /** Registry name; not emitted in the stats response. */
  public String getName() {
    return prefix + "_query_latency";
  }

  public String getTimeKey() {
    return prefix + "_query_time_in_millis";
  }

  public String getCountKey() {
    return prefix + "_query_timed_count";
  }
}
