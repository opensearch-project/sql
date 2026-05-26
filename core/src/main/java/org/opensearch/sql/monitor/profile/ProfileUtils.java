/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Utility helpers for profiling metrics. */
public final class ProfileUtils {

  private ProfileUtils() {}

  /**
   * Convert nanoseconds to milliseconds, rounded to two decimals.
   *
   * @param nanos duration in nanoseconds
   * @return rounded milliseconds
   */
  public static double roundToMillis(long nanos) {
    return Math.round((nanos / 1_000_000.0d) * 100.0d) / 100.0d;
  }
}
