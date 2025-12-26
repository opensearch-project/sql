/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import com.google.gson.annotations.SerializedName;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/** Immutable snapshot of query profiling metrics. */
@Getter
public final class QueryProfile {

  /** Total elapsed milliseconds for the profiled query (rounded to two decimals). */
  @SerializedName("total_ms")
  private final double totalMillis;

  /** Immutable metric values keyed by metric name in milliseconds (rounded to two decimals). */
  private final Map<String, Double> metrics;

  /**
   * Create a new query profile snapshot.
   *
   * @param totalMillis total elapsed milliseconds for the query (rounded to two decimals)
   * @param metrics metric values keyed by {@link MetricName}
   */
  public QueryProfile(double totalMillis, Map<MetricName, Double> metrics) {
    this.totalMillis = totalMillis;
    this.metrics = buildMetrics(metrics);
  }

  private Map<String, Double> buildMetrics(Map<MetricName, Double> metrics) {
    Objects.requireNonNull(metrics, "metrics");
    Map<String, Double> ordered = new java.util.LinkedHashMap<>(metrics.size());
    for (MetricName metricName : MetricName.values()) {
      Double value = metrics.getOrDefault(metricName, 0d);
      ordered.put(metricName.name() + "_MS", value);
    }
    return ordered;
  }
}
