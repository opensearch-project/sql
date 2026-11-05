/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Cumulative counter that accepts nanoseconds and reports whole milliseconds. Summing in nanos
 * avoids truncating every sub-millisecond query to zero.
 */
public class NanosToMillisCounter implements Counter<Long> {

  private final LongAdder nanos = new LongAdder();

  @Override
  public void increment() {
    nanos.increment();
  }

  @Override
  public void add(long n) {
    nanos.add(n);
  }

  @Override
  public Long getValue() {
    return TimeUnit.NANOSECONDS.toMillis(nanos.sum());
  }

  @Override
  public void reset() {
    nanos.reset();
  }
}
