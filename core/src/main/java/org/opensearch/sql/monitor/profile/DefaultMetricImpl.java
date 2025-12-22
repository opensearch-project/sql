/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.concurrent.atomic.LongAdder;

/** Concrete metric backed by {@link LongAdder}. */
final class DefaultMetricImpl implements ProfileMetric {

  private final String name;
  private final LongAdder value = new LongAdder();

  /**
   * Construct a metric with the provided name.
   *
   * @param name metric name
   */
  DefaultMetricImpl(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long value() {
    return value.sum();
  }

  @Override
  public void add(long delta) {
    value.add(delta);
  }

  @Override
  public void set(long value) {
    this.value.reset();
    this.value.add(value);
  }
}
