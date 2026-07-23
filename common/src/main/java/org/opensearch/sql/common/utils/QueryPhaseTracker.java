/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Tracks timing and resource usage for SQL/PPL query execution phases. Thread-local instance
 * collects phase durations (nanos) as the query flows through parse → analyze → plan → execute. The
 * result is serialized into a compact header value for query-insights to consume.
 *
 * <p>Phases that complete on the REST thread (parse) are stored in the Log4j ThreadContext via
 * {@link #persist()}, which is propagated to the sql-worker thread by OpenSearchQueryManager. On
 * the worker thread, {@link #startOrRestore()} restores prior phases and continues tracking.
 *
 * <p>CPU time uses {@code ThreadMXBean.getCurrentThreadCpuTime()}. Memory allocation tracking uses
 * {@code com.sun.management.ThreadMXBean.getThreadAllocatedBytes()} which is best-effort and may be
 * unavailable on non-HotSpot JVMs (e.g., IBM J9, GraalVM native).
 */
public final class QueryPhaseTracker {

  private static final String LOG4J_KEY = "_sql_phase_tracker";
  private static final ThreadLocal<QueryPhaseTracker> CURRENT = new ThreadLocal<>();
  private static final ThreadMXBean THREAD_MX = ManagementFactory.getThreadMXBean();
  private static final boolean CPU_SUPPORTED = THREAD_MX.isCurrentThreadCpuTimeSupported();
  private static final com.sun.management.ThreadMXBean SUN_THREAD_MX = getSunThreadMXBean();

  private final Map<String, Long> phases = new LinkedHashMap<>();
  private final Map<String, Long> cpuPhases = new LinkedHashMap<>();
  private final Map<String, Long> memPhases = new LinkedHashMap<>();
  private String activePhase;
  private long activeStart;
  private long activeCpuStart;
  private long activeMemStart;
  private long overallStart;

  private static com.sun.management.ThreadMXBean getSunThreadMXBean() {
    try {
      if (THREAD_MX instanceof com.sun.management.ThreadMXBean sun) {
        return sun;
      }
    } catch (NoClassDefFoundError | UnsupportedOperationException e) {
      // com.sun.management not available on this JVM
    }
    return null;
  }

  private QueryPhaseTracker() {
    this.overallStart = System.nanoTime();
  }

  public static QueryPhaseTracker start() {
    QueryPhaseTracker tracker = new QueryPhaseTracker();
    CURRENT.set(tracker);
    return tracker;
  }

  /** Restore from Log4j ThreadContext (cross-thread transfer) or create fresh. */
  public static QueryPhaseTracker startOrRestore() {
    QueryPhaseTracker tracker = new QueryPhaseTracker();
    String stored = org.apache.logging.log4j.ThreadContext.get(LOG4J_KEY);
    if (stored != null && !stored.isEmpty()) {
      try {
        for (String part : stored.split(",")) {
          // Format: "phase:wallNanos|cpu:cpuNanos|mem:memBytes"
          String[] segments = part.split("\\|");
          String[] kv = segments[0].split(":", 2);
          if (kv.length == 2) {
            tracker.phases.put(kv[0], Long.parseLong(kv[1]));
            for (int i = 1; i < segments.length; i++) {
              String[] metric = segments[i].split(":", 2);
              if (metric.length == 2 && "cpu".equals(metric[0])) {
                tracker.cpuPhases.put(kv[0], Long.parseLong(metric[1]));
              } else if (metric.length == 2 && "mem".equals(metric[0])) {
                tracker.memPhases.put(kv[0], Long.parseLong(metric[1]));
              }
            }
          }
        }
      } catch (NumberFormatException e) {
        // Malformed data in ThreadContext — start fresh
        tracker.phases.clear();
        tracker.cpuPhases.clear();
        tracker.memPhases.clear();
      }
    }
    CURRENT.set(tracker);
    return tracker;
  }

  public static QueryPhaseTracker current() {
    return CURRENT.get();
  }

  /** Returns true if no phases have been recorded (no prior SQL/PPL context). */
  public boolean isEmpty() {
    return phases.isEmpty() && activePhase == null;
  }

  public static void clear() {
    CURRENT.remove();
    org.apache.logging.log4j.ThreadContext.remove(LOG4J_KEY);
  }

  /** Persist current phases to Log4j ThreadContext for cross-thread propagation. */
  public void persist() {
    org.apache.logging.log4j.ThreadContext.put(LOG4J_KEY, serialize());
  }

  public void addCompletedPhase(String name, long nanos) {
    phases.put(name, nanos);
  }

  public void addCompletedPhase(String name, long nanos, long cpuNanos, long memBytes) {
    phases.put(name, nanos);
    if (cpuNanos > 0) cpuPhases.put(name, cpuNanos);
    if (memBytes > 0) memPhases.put(name, memBytes);
  }

  public void beginPhase(String name) {
    endCurrentPhase();
    activePhase = name;
    activeStart = System.nanoTime();
    activeCpuStart = CPU_SUPPORTED ? THREAD_MX.getCurrentThreadCpuTime() : 0;
    activeMemStart =
        SUN_THREAD_MX != null
            ? SUN_THREAD_MX.getThreadAllocatedBytes(Thread.currentThread().threadId())
            : 0;
  }

  public void endCurrentPhase() {
    if (activePhase != null) {
      long elapsed = System.nanoTime() - activeStart;
      phases.merge(activePhase, elapsed, Long::sum);
      if (CPU_SUPPORTED) {
        long cpuElapsed = THREAD_MX.getCurrentThreadCpuTime() - activeCpuStart;
        cpuPhases.merge(activePhase, cpuElapsed, Long::sum);
      }
      if (SUN_THREAD_MX != null) {
        long memElapsed =
            SUN_THREAD_MX.getThreadAllocatedBytes(Thread.currentThread().threadId())
                - activeMemStart;
        memPhases.merge(activePhase, memElapsed, Long::sum);
      }
      activePhase = null;
    }
  }

  public void endAll() {
    endCurrentPhase();
    long total = System.nanoTime() - overallStart;
    phases.put("total", total);
  }

  /**
   * Serialize to compact format with all metrics per phase:
   * "parse:1234|cpu:1000|mem:5000,analyze:5678|cpu:4000|mem:20000,total:18943" Time and CPU in
   * nanoseconds, memory in bytes.
   */
  public String serialize() {
    StringJoiner joiner = new StringJoiner(",");
    for (Map.Entry<String, Long> entry : phases.entrySet()) {
      String phase = entry.getKey();
      StringBuilder sb = new StringBuilder();
      sb.append(phase).append(':').append(entry.getValue());
      Long cpu = cpuPhases.get(phase);
      if (cpu != null) {
        sb.append("|cpu:").append(cpu);
      }
      Long mem = memPhases.get(phase);
      if (mem != null) {
        sb.append("|mem:").append(mem);
      }
      joiner.add(sb.toString());
    }
    return joiner.toString();
  }
}
