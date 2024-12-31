/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.util.function.Supplier;

/** Gauge metric, an instant value like cpu usage, state and so on */
public class GaugeMetric<T> extends Metric<T> {

  private final Supplier<T> loadValue;

  public GaugeMetric(String name, Supplier<T> supplier) {
    super(name);
    this.loadValue = supplier;
  }

  public String getName() {
    return super.getName();
  }

  public T getValue() {
    return loadValue.get();
  }
}
