/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import com.google.common.collect.ImmutableList;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.opensearch.sql.utils.DateTimeFormatters;

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
      throw new IllegalArgumentException("Cannot parse a null/empty date-time string.");
    }

    List<DateTimeFormatter> dateTimeFormatters =
        ImmutableList.of(DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
    List<DateTimeFormatter> dateFormatters = ImmutableList.of(DateTimeFormatter.ISO_DATE);
    List<DateTimeFormatter> timeFormatters = ImmutableList.of(DateTimeFormatter.ISO_TIME);

    if (input.contains(":")) {
      for (DateTimeFormatter fmt : dateTimeFormatters) {
        try {
          return LocalDateTime.parse(input, fmt);
        } catch (Exception ignored) {
        }
      }
      for (DateTimeFormatter fmt : timeFormatters) {
        try {
          LocalTime t = LocalTime.parse(input, fmt);
          return LocalDateTime.of(LocalDate.now(ZoneId.of("UTC")), t);
        } catch (Exception ignored) {
        }
      }
    } else {
      for (DateTimeFormatter fmt : dateFormatters) {
        try {
          LocalDate d = LocalDate.parse(input, fmt);
          return d.atStartOfDay();
        } catch (Exception ignored) {
        }
      }
    }
    throw new IllegalArgumentException(String.format("Unable to parse %s as datetime", input));
  }
}
