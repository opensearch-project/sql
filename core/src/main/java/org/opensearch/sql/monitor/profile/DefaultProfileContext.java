/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Default implementation that records profiling metrics. */
public class DefaultProfileContext implements ProfileContext {

  private final long startNanos = System.nanoTime();
  private boolean finished;
  private final Map<MetricName, DefaultMetricImpl> metrics = new ConcurrentHashMap<>();
  private QueryProfile profile;

  public DefaultProfileContext() {}

  /** {@inheritDoc} */
  @Override
  public ProfileMetric getOrCreateMetric(MetricName name) {
    Objects.requireNonNull(name, "name");
    return metrics.computeIfAbsent(name, key -> new DefaultMetricImpl(key.name()));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized QueryProfile finish() {
    if (finished) {
      return profile;
    }
    finished = true;
    long endNanos = System.nanoTime();
    Map<MetricName, Double> snapshot = new LinkedHashMap<>(MetricName.values().length);
    for (MetricName metricName : MetricName.values()) {
      DefaultMetricImpl metric = metrics.get(metricName);
      double millis = metric == null ? 0d : roundToMillis(metric.value());
      snapshot.put(metricName, millis);
    }
    double totalMillis = roundToMillis(endNanos - startNanos);
    profile = new QueryProfile(totalMillis, snapshot);
    return profile;
  }

  private double roundToMillis(long nanos) {
    return Math.round((nanos / 1_000_000.0d) * 100.0d) / 100.0d;
  }
}
