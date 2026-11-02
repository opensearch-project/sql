/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertNotEquals;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

/** Uses CPU on the calling thread, for tests that check a thread's CPU time was recorded. */
final class CpuBurner {

  /** Gives up after this long, so a JVM whose thread CPU clock never moves fails the test. */
  private static final long MAX_BURN_NANOS = TimeUnit.SECONDS.toNanos(5);

  private CpuBurner() {}

  /**
   * Uses CPU on this thread until the JVM's thread CPU clock has moved. A fixed amount of work is
   * not enough: Windows reports thread CPU time in timer ticks of about 15.6 ms, so a short loop
   * can finish without the clock moving, and a measurement around it would read 0.
   */
  static void burn() {
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    long start = threads.getCurrentThreadCpuTime();
    long deadline = System.nanoTime() + MAX_BURN_NANOS;
    long x = 0;
    do {
      for (int i = 0; i < 1_000_000; i++) {
        x += Integer.toString(i).hashCode();
      }
    } while (threads.getCurrentThreadCpuTime() == start && System.nanoTime() - deadline < 0);
    // Uses the result, so the loop is not optimized away.
    assertNotEquals(42, x);
  }
}
