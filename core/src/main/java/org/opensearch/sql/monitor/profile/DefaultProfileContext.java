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
  private ProfilePlanNode planRoot;
  private QueryProfile profile;

  public DefaultProfileContext() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public ProfileMetric getOrCreateMetric(MetricName name) {
    Objects.requireNonNull(name, "name");
    return metrics.computeIfAbsent(name, key -> new DefaultMetricImpl(key.name()));
  }

  @Override
  public synchronized void setPlanRoot(ProfilePlanNode planRoot) {
    if (this.planRoot == null) {
      this.planRoot = planRoot;
    }
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
      double millis = metric == null ? 0d : ProfileUtils.roundToMillis(metric.value());
      snapshot.put(metricName, millis);
    }
    double totalMillis = ProfileUtils.roundToMillis(endNanos - startNanos);
    QueryProfile.PlanNode planSnapshot = planRoot == null ? null : planRoot.snapshot();
    profile = new QueryProfile(totalMillis, snapshot, planSnapshot);
    return profile;
  }
}
