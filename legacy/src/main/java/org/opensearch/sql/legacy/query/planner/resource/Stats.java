/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.resource;

import org.opensearch.transport.client.Client;

/**
 * Statistics collector collects from OpenSearch stats, JVM etc for other components:
 *
 * <ol>
 *   <li>Resource monitor
 *   <li>Cost estimation
 *   <li>Block size calculation
 * </ol>
 */
public class Stats {

  /** Client connection to OpenSearch cluster (unused now) */
  private final Client client;

  public Stats(Client client) {
    this.client = client;
  }

  public MemStats collectMemStats() {
    return new MemStats(Runtime.getRuntime().freeMemory(), Runtime.getRuntime().totalMemory());
  }

  /** Statistics data class for memory usage */
  public static class MemStats {
    private final long free;
    private final long total;

    public MemStats(long free, long total) {
      this.free = free;
      this.total = total;
    }

    public long getFree() {
      return free;
    }

    public long getTotal() {
      return total;
    }
  }

  /*
  public class IndexStats {
      private long size;
      private long docNum;

      public IndexStats(long size, long docNum) {
          this.size = size;
          this.docNum = docNum;
      }
  }
  */

}
