/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Context for collecting profiling metrics during query execution. */
public interface ProfileContext extends AutoCloseable {
  /**
   * @return true when profiling is enabled.
   */
  boolean isEnabled();

  /**
   * Obtain or create a metric with the provided name.
   *
   * @param name fully qualified metric name
   * @return metric instance
   */
  ProfileMetric getOrCreateMetric(MetricName name);

  /**
   * Finalize profiling and return a snapshot.
   *
   * @return immutable query profile snapshot
   */
  QueryProfile finish();

  @Override
  default void close() {
    finish();
  }
}
