/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

/** Configuration for time units used in bin operations. */
public enum TimeUnitConfig {
  MICROSECONDS("us", 1, -1, true), // Special case: multiply by 1000
  MILLISECONDS("ms", 1, 1, true),
  CENTISECONDS("cs", 1, 10L, true),
  DECISECONDS("ds", 1, 100L, true),
  SECONDS("s", 1000, 1, true),
  MINUTES("m", 60000, 60, true),
  HOURS("h", 3600000, 3600, true),
  DAYS("d", 86400000, 1, false),
  MONTHS("M", 0, 1, false); // Special handling

  private final String unit;
  private final int multiplierMillis;
  private final long divisionFactor;
  private final boolean supportsAlignment;

  TimeUnitConfig(
      String unit, int multiplierMillis, long divisionFactor, boolean supportsAlignment) {
    this.unit = unit;
    this.multiplierMillis = multiplierMillis;
    this.divisionFactor = divisionFactor;
    this.supportsAlignment = supportsAlignment;
  }

  public String getUnit() {
    return unit;
  }

  public int getMultiplierMillis() {
    return multiplierMillis;
  }

  public long getDivisionFactor() {
    return divisionFactor;
  }

  public boolean supportsAlignment() {
    return supportsAlignment;
  }

  /** Converts interval value to milliseconds. */
  public long toMilliseconds(int intervalValue) {
    if (this == MICROSECONDS) {
      return intervalValue / 1000L;
    }
    return (long) intervalValue * multiplierMillis;
  }

  /** Converts interval value to seconds. */
  public long toSeconds(int intervalValue) {
    return toMilliseconds(intervalValue) / 1000L;
  }
}
