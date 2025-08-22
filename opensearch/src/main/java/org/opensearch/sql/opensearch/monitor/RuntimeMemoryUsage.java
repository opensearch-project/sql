/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

/** Get memory usage from runtime, which is used in v2. */
public class RuntimeMemoryUsage implements MemoryUsage {
  private RuntimeMemoryUsage() {}

  private static class Holder {
    static final MemoryUsage INSTANCE = new RuntimeMemoryUsage();
  }

  public static MemoryUsage getInstance() {
    return Holder.INSTANCE;
  }

  @Override
  public long usage() {
    final long freeMemory = Runtime.getRuntime().freeMemory();
    final long totalMemory = Runtime.getRuntime().totalMemory();
    return totalMemory - freeMemory;
  }

  @Override
  public void setUsage(long usage) {
    throw new UnsupportedOperationException("Cannot set usage in RuntimeMemoryUsage");
  }
}
