/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import org.opensearch.sql.utils.DateTimeFormatters;

public interface DateTimeParser {
  /**
   * Parse a string into a LocalDateTime. If only date is found, time is set to 00:00:00. If only
   * time is found, date is set to today at UTC.
   *
   * @param input A date/time/timestamp string
   * @return A LocalDateTime
   * @throws IllegalArgumentException if parsing fails
   */
  static LocalDateTime parse(String input) {
    try {
      return LocalDateTime.parse(input, DateTimeFormatters.DATE_TIMESTAMP_FORMATTER);
    } catch (DateTimeParseException ignored) {
    }

    try {
      LocalTime t = LocalTime.parse(input, DateTimeFormatters.TIME_TIMESTAMP_FORMATTER);
      return LocalDateTime.of(LocalDate.now(ZoneId.of("UTC")), t);
    } catch (Exception ignored) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "timestamp:%s in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'",
              input));
    }
  }
}
