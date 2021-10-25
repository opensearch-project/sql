/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DateTimeUtils {

  /**
   * Util method to round the date/time with given unit.
   *
   * @param utcMillis     Date/time value to round, given in utc millis
   * @param unitMillis    Date/time interval unit in utc millis
   * @return              Rounded date/time value in utc millis
   */
  public static long roundFloor(long utcMillis, long unitMillis) {
    return utcMillis - utcMillis % unitMillis;
  }

  /**
   * Util method to round the date/time in week(s).
   *
   * @param utcMillis     Date/time value to round, given in utc millis
   * @param interval      Number of weeks as the rounding interval
   * @return              Rounded date/time value in utc millis
   */
  public static long roundWeek(long utcMillis, int interval) {
    return roundFloor(utcMillis + 259200000L, 604800000L * interval) - 259200000L;
  }

  /**
   * Util method to round the date/time in month(s).
   *
   * @param utcMillis     Date/time value to round, given in utc millis
   * @param interval      Number of months as the rounding interval
   * @return              Rounded date/time value in utc millis
   */
  public static long roundMonth(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneId.of("UTC"))
        .plusMonths(interval);
    long monthDiff = (zonedDateTime.getYear() - initDateTime.getYear()) * 12L + zonedDateTime
        .getMonthValue() - initDateTime.getMonthValue();
    long monthToAdd = (monthDiff / interval - 1) * interval;
    return initDateTime.plusMonths(monthToAdd).toInstant().toEpochMilli();
  }

  /**
   * Util method to round the date/time in quarter(s).
   *
   * @param utcMillis     Date/time value to round, given in utc millis
   * @param interval      Number of quarters as the rounding interval
   * @return              Rounded date/time value in utc millis
   */
  public static long roundQuarter(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneId.of("UTC"))
        .plusMonths(interval * 3L);
    long monthDiff = ((zonedDateTime.getYear() - initDateTime.getYear()) * 12L + zonedDateTime
        .getMonthValue() - initDateTime.getMonthValue());
    long monthToAdd = (monthDiff / (interval * 3L) - 1) * interval * 3;
    return initDateTime.plusMonths(monthToAdd).toInstant().toEpochMilli();
  }

  /**
   * Util method to round the date/time in year(s).
   *
   * @param utcMillis     Date/time value to round, given in utc millis
   * @param interval      Number of years as the rounding interval
   * @return              Rounded date/time value in utc millis
   */
  public static long roundYear(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneId.of("UTC"));
    int yearDiff = zonedDateTime.getYear() - initDateTime.getYear();
    int yearToAdd = (yearDiff / interval) * interval;
    return initDateTime.plusYears(yearToAdd).toInstant().toEpochMilli();
  }

}
