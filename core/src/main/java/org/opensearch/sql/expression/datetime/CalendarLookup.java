/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import com.google.common.collect.ImmutableList;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Calendar;
import lombok.AllArgsConstructor;
import org.opensearch.sql.exception.SemanticCheckException;

@AllArgsConstructor
class CalendarLookup {

  /**
   * Get a calendar for the specific mode.
   *
   * @param mode Mode to get calendar for.
   * @param date Date to get calendar for.
   */
  private static Calendar getCalendar(int mode, LocalDate date) {
    if ((mode < 0) || (mode > 7)) {
      throw new SemanticCheckException(
          String.format("mode:%s is invalid, please use mode value between 0-7", mode));
    }
    int day = (mode % 2 == 0) ? Calendar.SUNDAY : Calendar.MONDAY;
    if (ImmutableList.of(1, 3).contains(mode)) {
      return getCalendar(day, 5, date);
    } else if (ImmutableList.of(4, 6).contains(mode)) {
      return getCalendar(day, 4, date);
    } else {
      return getCalendar(day, 7, date);
    }
  }

  /**
   * Set first day of week, minimal days in first week and date in calendar.
   *
   * @param firstDayOfWeek the given first day of the week.
   * @param minimalDaysInWeek the given minimal days required in the first week of the year.
   * @param date the given date.
   */
  private static Calendar getCalendar(int firstDayOfWeek, int minimalDaysInWeek, LocalDate date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setFirstDayOfWeek(firstDayOfWeek);
    calendar.setMinimalDaysInFirstWeek(minimalDaysInWeek);
    calendar.set(date.getYear(), date.getMonthValue() - 1, date.getDayOfMonth());
    return calendar;
  }

  /**
   * Returns week number for date according to mode.
   *
   * @param mode Integer for mode. Valid mode values are 0 to 7.
   * @param date LocalDate for date.
   */
  static int getWeekNumber(int mode, LocalDate date) {
    Calendar calendar = getCalendar(mode, date);
    int weekNumber = calendar.get(Calendar.WEEK_OF_YEAR);
    if ((weekNumber > 51)
        && (calendar.get(Calendar.DAY_OF_MONTH) < 7)
        && Arrays.asList(0, 1, 4, 5).contains(mode)) {
      weekNumber = 0;
    }
    return weekNumber;
  }

  /**
   * Returns year for date according to mode.
   *
   * @param mode Integer for mode. Valid mode values are 0 to 7.
   * @param date LocalDate for date.
   */
  static int getYearNumber(int mode, LocalDate date) {
    Calendar calendar = getCalendar(mode, date);
    int weekNumber = getWeekNumber(mode, date);
    int yearNumber = calendar.get(Calendar.YEAR);
    if ((weekNumber > 51) && (calendar.get(Calendar.DAY_OF_MONTH) < 7)) {
      yearNumber--;
    }
    return yearNumber;
  }
}
