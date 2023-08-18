/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.time.Clock;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

/**
 * Rolling counter. The count is refreshed every interval. In every interval the count is
 * cumulative.
 */
public class RollingCounter implements Counter<Long> {

  private final long capacity;
  private final long window;
  private final long interval;
  private final Clock clock;
  private final ConcurrentSkipListMap<Long, Long> time2CountWin;
  private final LongAdder count;

  public RollingCounter() {
    this(
        LocalClusterState.state().getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW),
        LocalClusterState.state().getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL));
  }

  public RollingCounter(long window, long interval, Clock clock) {
    this.window = window;
    this.interval = interval;
    this.clock = clock;
    time2CountWin = new ConcurrentSkipListMap<>();
    count = new LongAdder();
    capacity = window / interval * 2;
  }

  public RollingCounter(long window, long interval) {
    this(window, interval, Clock.systemDefaultZone());
  }

  @Override
  public void increment() {
    add(1L);
  }

  @Override
  public void add(long n) {
    trim();
    time2CountWin.compute(getKey(clock.millis()), (k, v) -> (v == null) ? n : v + n);
  }

  @Override
  public Long getValue() {
    return getValue(getPreKey(clock.millis()));
  }

  public long getValue(long key) {
    Long res = time2CountWin.get(key);
    if (res == null) {
      return 0;
    }

    return res;
  }

  public long getSum() {
    return count.longValue();
  }

  private void trim() {
    if (time2CountWin.size() > capacity) {
      time2CountWin.headMap(getKey(clock.millis() - window * 1000)).clear();
    }
  }

  private long getKey(long millis) {
    return millis / 1000 / this.interval;
  }

  private long getPreKey(long millis) {
    return getKey(millis) - 1;
  }

  public int size() {
    return time2CountWin.size();
  }

  public void reset() {
    time2CountWin.clear();
  }
}
