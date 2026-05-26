/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

import java.util.HashMap;
import java.util.Map;

/** Registry for time unit configurations and mappings. */
public class TimeUnitRegistry {

  private static final Map<String, TimeUnitConfig> UNIT_MAPPING = new HashMap<>();

  static {
    // Microseconds (case-sensitive, lowercase only)
    UNIT_MAPPING.put("us", TimeUnitConfig.MICROSECONDS);

    // Milliseconds
    UNIT_MAPPING.put("ms", TimeUnitConfig.MILLISECONDS);

    // Centiseconds (case-sensitive, lowercase only)
    UNIT_MAPPING.put("cs", TimeUnitConfig.CENTISECONDS);

    // Deciseconds (case-sensitive, lowercase only)
    UNIT_MAPPING.put("ds", TimeUnitConfig.DECISECONDS);

    // Seconds
    UNIT_MAPPING.put("s", TimeUnitConfig.SECONDS);
    UNIT_MAPPING.put("sec", TimeUnitConfig.SECONDS);
    UNIT_MAPPING.put("secs", TimeUnitConfig.SECONDS);
    UNIT_MAPPING.put("second", TimeUnitConfig.SECONDS);
    UNIT_MAPPING.put("seconds", TimeUnitConfig.SECONDS);

    // Minutes (case-sensitive lowercase 'm')
    UNIT_MAPPING.put("m", TimeUnitConfig.MINUTES);
    UNIT_MAPPING.put("min", TimeUnitConfig.MINUTES);
    UNIT_MAPPING.put("mins", TimeUnitConfig.MINUTES);
    UNIT_MAPPING.put("minute", TimeUnitConfig.MINUTES);
    UNIT_MAPPING.put("minutes", TimeUnitConfig.MINUTES);

    // Hours
    UNIT_MAPPING.put("h", TimeUnitConfig.HOURS);
    UNIT_MAPPING.put("hr", TimeUnitConfig.HOURS);
    UNIT_MAPPING.put("hrs", TimeUnitConfig.HOURS);
    UNIT_MAPPING.put("hour", TimeUnitConfig.HOURS);
    UNIT_MAPPING.put("hours", TimeUnitConfig.HOURS);

    // Days
    UNIT_MAPPING.put("d", TimeUnitConfig.DAYS);
    UNIT_MAPPING.put("day", TimeUnitConfig.DAYS);
    UNIT_MAPPING.put("days", TimeUnitConfig.DAYS);

    // Months (case-sensitive uppercase 'M')
    UNIT_MAPPING.put("M", TimeUnitConfig.MONTHS);
    UNIT_MAPPING.put("mon", TimeUnitConfig.MONTHS);
    UNIT_MAPPING.put("month", TimeUnitConfig.MONTHS);
    UNIT_MAPPING.put("months", TimeUnitConfig.MONTHS);
  }

  /**
   * Gets the time unit configuration for the given unit string.
   *
   * @param unit The unit string (e.g., "h", "hours", "M", "m")
   * @return The time unit configuration, or null if not found
   */
  public static TimeUnitConfig getConfig(String unit) {
    // Handle case-sensitive units: M (month), m (minute), and subsecond units (us, cs, ds)
    if (unit.equals("M")
        || unit.equals("m")
        || unit.equals("us")
        || unit.equals("cs")
        || unit.equals("ds")) {
      return UNIT_MAPPING.get(unit);
    } else {
      // For all other units, use lowercase lookup for case-insensitive matching
      return UNIT_MAPPING.get(unit.toLowerCase());
    }
  }

  /**
   * Validates sub-second span constraints. When span is expressed using a sub-second unit, the span
   * value needs to be < 1 second, and 1 second must be evenly divisible by the span value.
   */
  public static void validateSubSecondSpan(TimeUnitConfig config, int intervalValue) {
    if (!isSubSecondUnit(config)) {
      return;
    }

    // Convert interval to microseconds for comparison
    long intervalMicros =
        switch (config) {
          case MICROSECONDS -> intervalValue;
          case MILLISECONDS -> intervalValue * 1000L;
          case CENTISECONDS -> intervalValue * 10000L;
          case DECISECONDS -> intervalValue * 100000L;
          default -> 0L;
        };

    long oneSecondMicros = 1000000L;

    // Constraint 1: span value must be < 1 second
    if (intervalMicros >= oneSecondMicros) {
      throw new IllegalArgumentException(
          String.format(
              "Sub-second span %d%s must be less than 1 second", intervalValue, config.getUnit()));
    }

    // Constraint 2: 1 second must be evenly divisible by the span value
    if (oneSecondMicros % intervalMicros != 0) {
      throw new IllegalArgumentException(
          String.format(
              "1 second must be evenly divisible by span %d%s", intervalValue, config.getUnit()));
    }
  }

  private static boolean isSubSecondUnit(TimeUnitConfig config) {
    return config == TimeUnitConfig.MICROSECONDS
        || config == TimeUnitConfig.MILLISECONDS
        || config == TimeUnitConfig.CENTISECONDS
        || config == TimeUnitConfig.DECISECONDS;
  }
}
