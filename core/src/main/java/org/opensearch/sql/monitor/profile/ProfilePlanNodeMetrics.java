/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.concurrent.atomic.LongAdder;

/** Metrics captured for a single plan node. */
public final class ProfilePlanNodeMetrics {

  private final LongAdder timeNanos = new LongAdder();
  private final LongAdder rows = new LongAdder();

  public ProfilePlanNodeMetrics() {}

  public void addTimeNanos(long nanos) {
    if (nanos > 0) {
      timeNanos.add(nanos);
    }
  }

  public void incrementRows() {
    rows.increment();
  }

  public long timeNanos() {
    return timeNanos.sum();
  }

  public long rows() {
    return rows.sum();
  }
}
