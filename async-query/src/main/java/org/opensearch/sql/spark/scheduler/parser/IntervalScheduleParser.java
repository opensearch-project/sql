/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.parser;

import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

/** Parse string raw schedule into job scheduler IntervalSchedule */
public class IntervalScheduleParser {
  private static final Pattern DURATION_PATTERN =
      Pattern.compile(
          "^(\\d+)\\s*(years?|months?|weeks?|days?|hours?|minutes?|minute|mins?|seconds?|secs?|milliseconds?|millis?|microseconds?|microsecond|micros?|micros|nanoseconds?|nanos?)$",
          Pattern.CASE_INSENSITIVE);

  public static Schedule parse(Object schedule, Instant startTime) {
    if (schedule == null) {
      return null;
    }

    if (schedule instanceof Schedule) {
      return (Schedule) schedule;
    }

    if (!(schedule instanceof String)) {
      throw new IllegalArgumentException("Schedule must be a String object for parsing.");
    }

    String intervalStr = ((String) schedule).trim().toLowerCase();

    Matcher matcher = DURATION_PATTERN.matcher(intervalStr);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid interval format: " + intervalStr);
    }

    long value = Long.parseLong(matcher.group(1));
    String unitStr = matcher.group(2).toLowerCase();

    // Convert to a supported unit or directly return an IntervalSchedule
    long intervalInMinutes = convertToSupportedUnit(value, unitStr);

    return new IntervalSchedule(startTime, (int) intervalInMinutes, ChronoUnit.MINUTES);
  }

  @VisibleForTesting
  protected static long convertToSupportedUnit(long value, String unitStr) {
    switch (unitStr) {
      case "years":
      case "year":
        throw new IllegalArgumentException("Years cannot be converted to minutes accurately.");
      case "months":
      case "month":
        throw new IllegalArgumentException("Months cannot be converted to minutes accurately.");
      case "weeks":
      case "week":
        return value * 7 * 24 * 60; // Convert weeks to minutes
      case "days":
      case "day":
        return value * 24 * 60; // Convert days to minutes
      case "hours":
      case "hour":
        return value * 60; // Convert hours to minutes
      case "minutes":
      case "minute":
      case "mins":
      case "min":
        return value; // Already in minutes
      case "seconds":
      case "second":
      case "secs":
      case "sec":
        return value / 60; // Convert seconds to minutes
      case "milliseconds":
      case "millisecond":
      case "millis":
      case "milli":
        return value / (60 * 1000); // Convert milliseconds to minutes
      case "microseconds":
      case "microsecond":
      case "micros":
      case "micro":
        return value / (60 * 1000 * 1000); // Convert microseconds to minutes
      case "nanoseconds":
      case "nanosecond":
      case "nanos":
      case "nano":
        return value / (60 * 1000 * 1000 * 1000L); // Convert nanoseconds to minutes
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unitStr);
    }
  }
}
