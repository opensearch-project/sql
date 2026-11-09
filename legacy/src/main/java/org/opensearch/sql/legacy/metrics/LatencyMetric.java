/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.util.concurrent.TimeUnit;
import org.json.JSONObject;

/**
 * Cumulative query latency: total time and the number of queries it covers, since node start.
 *
 * <p>Both values are updated and read under one lock, so a stats response always pairs a total time
 * with exactly the count that produced it. Consumers can compute average latency over an interval
 * as {@code delta(time) / delta(count)} without seeing time move while count doesn't. Both values
 * reset to zero on node restart.
 *
 * <p>The only write operation is {@link #record(long)}: there is deliberately no {@code
 * increment()}, so callers can't bump the count without supplying an elapsed time.
 */
public class LatencyMetric extends Metric<LatencyMetric.Snapshot> {

  private static final long serialVersionUID = 1L;

  private final LatencyMetricName latencyName;
  private long totalNanos;
  private long count;

  public LatencyMetric(LatencyMetricName latencyName) {
    super(latencyName.getName());
    this.latencyName = latencyName;
  }

  /** Record one completed query. Time is kept in nanos so sub-millisecond queries add up. */
  public synchronized void record(long elapsedNanos) {
    totalNanos += Math.max(0L, elapsedNanos);
    count++;
  }

  @Override
  public synchronized Snapshot getValue() {
    return new Snapshot(totalNanos, count);
  }

  @Override
  public void collectInto(JSONObject json) {
    Snapshot snapshot = getValue();
    json.put(latencyName.getTimeKey(), snapshot.getTotalMillis());
    json.put(latencyName.getCountKey(), snapshot.getCount());
  }

  /** A consistent (time, count) pair. */
  public static final class Snapshot {
    private final long totalNanos;
    private final long count;

    Snapshot(long totalNanos, long count) {
      this.totalNanos = totalNanos;
      this.count = count;
    }

    public long getTotalNanos() {
      return totalNanos;
    }

    public long getTotalMillis() {
      return TimeUnit.NANOSECONDS.toMillis(totalNanos);
    }

    public long getCount() {
      return count;
    }
  }
}
