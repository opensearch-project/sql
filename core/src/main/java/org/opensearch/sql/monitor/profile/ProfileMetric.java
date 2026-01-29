/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Metric for query profiling. */
public interface ProfileMetric {
  /**
   * @return metric name.
   */
  String name();

  /**
   * @return current metric value.
   */
  long value();

  /**
   * Increment the metric by the given delta.
   *
   * @param delta amount to add
   */
  void add(long delta);

  /**
   * Set the metric to the provided value.
   *
   * @param value new metric value
   */
  void set(long value);
}
