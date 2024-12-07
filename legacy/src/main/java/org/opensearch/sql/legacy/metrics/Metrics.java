/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

public class Metrics {

  private static final Metrics metrics = new Metrics();
  private final ConcurrentHashMap<String, Metric> registeredMetricsByName =
      new ConcurrentHashMap<>();

  public static Metrics getInstance() {
    return metrics;
  }

  private Metrics() {}

  public void registerDefaultMetrics() {
    for (MetricName metricName : MetricName.values()) {
      registerMetric(MetricFactory.createMetric(metricName));
    }
  }

  public void registerMetric(Metric metric) {
    registeredMetricsByName.put(metric.getName(), metric);
  }

  public void unregisterMetric(String name) {
    if (name == null) {
      return;
    }

    registeredMetricsByName.remove(name);
  }

  public Metric getMetric(String name) {
    if (name == null) {
      return null;
    }

    return registeredMetricsByName.get(name);
  }

  public NumericMetric getNumericalMetric(MetricName metricName) {
    String name = metricName.getName();
    if (!metricName.isNumerical()) {
      name = MetricName.DEFAULT.getName();
    }

    return (NumericMetric) registeredMetricsByName.get(name);
  }

  public List<Metric> getAllMetrics() {
    return new ArrayList<>(registeredMetricsByName.values());
  }

  public String collectToJSON() {
    JSONObject metricsJSONObject = new JSONObject();

    for (Metric metric : registeredMetricsByName.values()) {
      if (metric.getName().equals("default")) {
        continue;
      }
      metricsJSONObject.put(metric.getName(), metric.getValue());
    }

    return metricsJSONObject.toString();
  }

  public void clear() {
    registeredMetricsByName.clear();
  }
}
