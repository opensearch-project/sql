/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import java.time.*;
import org.apache.calcite.sql.type.SqlTypeName;

public interface InstantUtils {

  /**
   * Convert epoch milliseconds to Instant.
   *
   * @param epochMillis epoch milliseconds
   * @return Instant that represents the given epoch milliseconds
   */
  public static Instant fromEpochMills(long epochMillis) {
    return Instant.ofEpochMilli(epochMillis);
  }

  /**
   * Convert internal date to Instant.
   *
   * @param date internal date in days since epoch
   * @return Instant that represents the given date at timezone UTC at 00:00:00
   */
  static Instant fromInternalDate(int date) {
    LocalDate localDate = LocalDate.ofEpochDay(date);
    return localDate.atStartOfDay(ZoneId.of("UTC")).toInstant();
  }

  /**
   * Convert internal time to Instant.
   *
   * @param time internal time in milliseconds
   * @return Instant that represents the current day with the given time at timezone UTC
   */
  static Instant fromInternalTime(int time) {
    LocalDate todayUtc = LocalDate.now(ZoneId.of("UTC"));
    ZonedDateTime startOfDayUtc = todayUtc.atStartOfDay(ZoneId.of("UTC"));

    return startOfDayUtc.toInstant().plus(Duration.ofMillis(time));
  }

  static Instant fromStringExpr(String timestampExpression) {
    LocalDateTime datetime = DateTimeParser.parse(timestampExpression);
    return datetime.atZone(ZoneId.of("UTC")).toInstant();
  }

  /**
   * Convert internal date/time/timestamp to Instant.
   *
   * @param candidate internal date/time/timestamp. Date is represented as days since epoch, time is
   *     represented as milliseconds, and timestamp is represented as epoch milliseconds
   * @param sqlTypeName type of the internalDatetime
   * @return Instant that represents the given internalDatetime
   */
  static Instant convertToInstant(Object candidate, SqlTypeName sqlTypeName) {
    Instant dateTimeBase;
    switch (sqlTypeName) {
      case DATE:
        dateTimeBase = InstantUtils.fromInternalDate((int) candidate);
        break;
      case TIMESTAMP:
        dateTimeBase = InstantUtils.fromEpochMills((long) candidate);
        break;
      case TIME:
        dateTimeBase = InstantUtils.fromInternalTime((int) candidate);
        break;
      default:
        String timestampExpression = (String) candidate;
        dateTimeBase = InstantUtils.fromStringExpr(timestampExpression);
    }
    return dateTimeBase;
  }
}
