/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.util.concurrent.atomic.LongAdder;

public class BasicCounter implements Counter<Long> {

  private final LongAdder count = new LongAdder();

  @Override
  public void increment() {
    count.increment();
  }

  @Override
  public void add(long n) {
    count.add(n);
  }

  @Override
  public Long getValue() {
    return count.longValue();
  }

  @Override
  public void reset() {
    count.reset();
  }
}
