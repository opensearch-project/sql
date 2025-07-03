/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_DATE_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_TIMESTAMP_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_TIME_FORMATTER;

import com.google.common.collect.ImmutableList;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.opensearch.sql.exception.SemanticCheckException;

public interface DateTimeParser {
  /**
   * Parse a string into a LocalDateTime If only date is found, time is set to 00:00:00. If only
   * time is found, date is set to today.
   *
   * @param input A date/time/timestamp string
   * @return A LocalDateTime
   * @throws IllegalArgumentException if parsing fails
   */
  static LocalDateTime parse(String input) {

    if (input == null || input.trim().isEmpty()) {
      throw new SemanticCheckException("Cannot parse a null/empty date-time string.");
    }

    if (input.contains(":")) {
      try {
        return parseTimestamp(input);
      } catch (Exception ignored) {
      }

      try {
        LocalTime t = parseTime(input);
        return LocalDateTime.of(LocalDate.now(ZoneId.of("UTC")), t);
      } catch (Exception ignored) {
      }
    } else {
      try {
        LocalDate d = parseDate(input);
        return d.atStartOfDay();
      } catch (Exception ignored) {
      }
    }
    throw new SemanticCheckException(String.format("Unable to parse %s as datetime", input));
  }

  static LocalDateTime parseTimeOrTimestamp(String input) {
    if (input == null || input.trim().isEmpty()) {
      throw new SemanticCheckException("Cannot parse a null/empty date-time string.");
    }

    try {
      return parseTime(input).atDate(LocalDate.now(ZoneId.of("UTC")));
    } catch (Exception ignored) {
    }

    try {
      return parseTimestamp(input);
    } catch (Exception ignored) {
    }

    throw new SemanticCheckException(
        String.format("time:%s in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'", input));
  }

  static LocalDateTime parseDateOrTimestamp(String input) {
    if (input == null || input.trim().isEmpty()) {
      throw new SemanticCheckException("Cannot parse a null/empty date-time string.");
    }

    try {
      return parseDate(input).atStartOfDay();
    } catch (Exception ignored) {
    }

    try {
      return parseTimestamp(input);
    } catch (Exception ignored) {
    }

    throw new SemanticCheckException(
        String.format("date:%s in unsupported format, please use 'yyyy-MM-dd'", input));
  }

  static LocalDateTime parseTimestamp(String input) {
    List<DateTimeFormatter> dateTimeFormatters = ImmutableList.of(STRICT_TIMESTAMP_FORMATTER);

    for (DateTimeFormatter fmt : dateTimeFormatters) {
      try {
        return LocalDateTime.parse(input, fmt);
      } catch (Exception ignored) {
      }
    }
    throw new SemanticCheckException(
        String.format(
            "timestamp:%s in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'",
            input));
  }

  static LocalTime parseTime(String input) {
    List<DateTimeFormatter> timeFormatters = ImmutableList.of(STRICT_TIME_FORMATTER);
    for (DateTimeFormatter fmt : timeFormatters) {
      try {
        return LocalTime.parse(input, fmt);
      } catch (Exception ignored) {
      }
    }
    throw new SemanticCheckException(
        String.format("time:%s in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'", input));
  }

  static LocalDate parseDate(String input) {
    List<DateTimeFormatter> dateFormatters = ImmutableList.of(STRICT_DATE_FORMATTER);
    for (DateTimeFormatter fmt : dateFormatters) {
      try {
        return LocalDate.parse(input, fmt);
      } catch (Exception ignored) {
      }
    }
    throw new SemanticCheckException(
        String.format("date:%s in unsupported format, please use 'yyyy-MM-dd'", input));
  }
}
