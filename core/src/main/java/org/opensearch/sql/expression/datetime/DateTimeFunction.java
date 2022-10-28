/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;
import static org.opensearch.sql.expression.function.FunctionDSL.queryContextFunction;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_LONG_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_SHORT_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_LONG_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_SHORT_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_STRICT_WITH_TZ;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.utils.DateTimeUtils;

/**
 * The definition of date and time functions.
 * 1) have the clear interface for function define.
 * 2) the implementation should rely on ExprValue.
 */
@UtilityClass
public class DateTimeFunction {
  // The number of days from year zero to year 1970.
  private static final Long DAYS_0000_TO_1970 = (146097 * 5L) - (30L * 365L + 7L);

  // MySQL doesn't process any datetime/timestamp values which are greater than
  // 32536771199.999999, or equivalent '3001-01-18 23:59:59.999999' UTC
  private static final Double MYSQL_MAX_TIMESTAMP = 32536771200d;

  /**
   * Register Date and Time Functions.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(adddate());
    repository.register(convert_tz());
    repository.register(date());
    repository.register(datetime());
    repository.register(date_add());
    repository.register(date_sub());
    repository.register(day());
    repository.register(dayName());
    repository.register(dayOfMonth());
    repository.register(dayOfWeek());
    repository.register(dayOfYear());
    repository.register(from_days());
    repository.register(from_unixtime());
    repository.register(hour());
    repository.register(makedate());
    repository.register(maketime());
    repository.register(microsecond());
    repository.register(minute());
    repository.register(month());
    repository.register(monthName());
    repository.register(quarter());
    repository.register(second());
    repository.register(subdate());
    repository.register(time());
    repository.register(time_to_sec());
    repository.register(timestamp());
    repository.register(date_format());
    repository.register(to_days());
    repository.register(unix_timestamp());
    repository.register(week());
    repository.register(year());

    repository.register(now());
    repository.register(current_timestamp());
    repository.register(localtimestamp());
    repository.register(localtime());
    repository.register(sysdate());
    repository.register(curtime());
    repository.register(current_time());
    repository.register(curdate());
    repository.register(current_date());
  }

  /**
   * NOW() returns a constant time that indicates the time at which the statement began to execute.
   * `fsp` argument support is removed until refactoring to avoid bug where `now()`, `now(x)` and
   * `now(y) return different values.
   */
  private FunctionResolver now(FunctionName functionName) {
    return define(functionName,
        queryContextFunction(
            functionProperties -> new ExprDatetimeValue(
                formatNow(functionProperties.getQueryStartClock())), DATETIME)
    );
  }

  private FunctionResolver now() {
    return now(BuiltinFunctionName.NOW.getName());
  }

  private FunctionResolver current_timestamp() {
    return now(BuiltinFunctionName.CURRENT_TIMESTAMP.getName());
  }

  private FunctionResolver localtimestamp() {
    return now(BuiltinFunctionName.LOCALTIMESTAMP.getName());
  }

  private FunctionResolver localtime() {
    return now(BuiltinFunctionName.LOCALTIME.getName());
  }

  /**
   * SYSDATE() returns the time at which it executes.
   */
  private FunctionResolver sysdate() {
    return define(BuiltinFunctionName.SYSDATE.getName(),
        queryContextFunction(functionProperties
            -> new ExprDatetimeValue(formatNow(Clock.systemDefaultZone())), DATETIME),
        queryContextFunction((functionProperties, v) -> new ExprDatetimeValue(
            formatNow(Clock.systemDefaultZone(), v.integerValue())), DATETIME, INTEGER)
    );
  }

  /**
   * Synonym for @see `now`.
   */
  private FunctionResolver curtime(FunctionName functionName) {
    return define(functionName,
        queryContextFunction(functionProperties -> new ExprTimeValue(
            formatNow(functionProperties.getQueryStartClock()).toLocalTime()), TIME));
  }

  private FunctionResolver curtime() {
    return curtime(BuiltinFunctionName.CURTIME.getName());
  }

  private FunctionResolver current_time() {
    return curtime(BuiltinFunctionName.CURRENT_TIME.getName());
  }

  private FunctionResolver curdate(FunctionName functionName) {
    return define(functionName,
        queryContextFunction(functionProperties -> new ExprDateValue(
            formatNow(functionProperties.getQueryStartClock()).toLocalDate()), DATE));
  }

  private FunctionResolver curdate() {
    return curdate(BuiltinFunctionName.CURDATE.getName());
  }

  private FunctionResolver current_date() {
    return curdate(BuiltinFunctionName.CURRENT_DATE.getName());
  }

  /**
   * Specify a start date and add a temporal amount to the date.
   * The return type depends on the date type and the interval unit. Detailed supported signatures:
   * (STRING/DATE/DATETIME/TIMESTAMP, INTERVAL) -> DATETIME
   * (DATE, LONG) -> DATE
   * (STRING/DATETIME/TIMESTAMP, LONG) -> DATETIME
   */

  private DefaultFunctionResolver add_date(FunctionName functionName) {
    return define(functionName,
        impl(nullMissingHandling(DateTimeFunction::exprAddDateInterval),
            DATETIME, STRING, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateInterval), DATETIME, DATE, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateInterval),
            DATETIME, DATETIME, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateInterval),
            DATETIME, TIMESTAMP, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateDays), DATE, DATE, LONG),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateDays), DATETIME, DATETIME, LONG),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateDays), DATETIME, TIMESTAMP, LONG),
        impl(nullMissingHandling(DateTimeFunction::exprAddDateDays), DATETIME, STRING, LONG)
    );
  }

  private DefaultFunctionResolver adddate() {
    return add_date(BuiltinFunctionName.ADDDATE.getName());
  }

  /**
   * Converts date/time from a specified timezone to another specified timezone.
   * The supported signatures:
   * (DATETIME, STRING, STRING) -> DATETIME
   * (STRING, STRING, STRING) -> DATETIME
   */
  private DefaultFunctionResolver convert_tz() {
    return define(BuiltinFunctionName.CONVERT_TZ.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprConvertTZ),
            DATETIME, DATETIME, STRING, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprConvertTZ),
            DATETIME, STRING, STRING, STRING)
    );
  }

  /**
   * Extracts the date part of a date and time value.
   * Also to construct a date type. The supported signatures:
   * STRING/DATE/DATETIME/TIMESTAMP -> DATE
   */
  private DefaultFunctionResolver date() {
    return define(BuiltinFunctionName.DATE.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, TIMESTAMP));
  }

  /**
   * Specify a datetime with time zone field and a time zone to convert to.
   * Returns a local date time.
   * (STRING, STRING) -> DATETIME
   * (STRING) -> DATETIME
   */
  private FunctionResolver datetime() {
    return define(BuiltinFunctionName.DATETIME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDateTime),
            DATETIME, STRING, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprDateTimeNoTimezone),
            DATETIME, STRING)
    );
  }

  private DefaultFunctionResolver date_add() {
    return add_date(BuiltinFunctionName.DATE_ADD.getName());
  }

  /**
   * Specify a start date and subtract a temporal amount to the date.
   * The return type depends on the date type and the interval unit. Detailed supported signatures:
   * (STRING/DATE/DATETIME/TIMESTAMP, INTERVAL) -> DATETIME
   * (DATE, LONG) -> DATE
   * (STRING/DATETIME/TIMESTAMP, LONG) -> DATETIME
   */
  private DefaultFunctionResolver sub_date(FunctionName functionName) {
    return define(functionName,
        impl(nullMissingHandling(DateTimeFunction::exprSubDateInterval),
            DATETIME, STRING, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateInterval), DATETIME, DATE, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateInterval),
            DATETIME, DATETIME, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateInterval),
            DATETIME, TIMESTAMP, INTERVAL),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateDays), DATE, DATE, LONG),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateDays), DATETIME, DATETIME, LONG),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateDays), DATETIME, TIMESTAMP, LONG),
        impl(nullMissingHandling(DateTimeFunction::exprSubDateDays), DATETIME, STRING, LONG)
    );
  }

  private DefaultFunctionResolver date_sub() {
    return sub_date(BuiltinFunctionName.DATE_SUB.getName());
  }

  /**
   * DAY(STRING/DATE/DATETIME/TIMESTAMP). return the day of the month (1-31).
   */
  private DefaultFunctionResolver day() {
    return define(BuiltinFunctionName.DAY.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, STRING)
    );
  }

  /**
   * DAYNAME(STRING/DATE/DATETIME/TIMESTAMP).
   * return the name of the weekday for date, including Monday, Tuesday, Wednesday,
   * Thursday, Friday, Saturday and Sunday.
   */
  private DefaultFunctionResolver dayName() {
    return define(BuiltinFunctionName.DAYNAME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDayName), STRING, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDayName), STRING, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDayName), STRING, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprDayName), STRING, STRING)
    );
  }

  /**
   * DAYOFMONTH(STRING/DATE/DATETIME/TIMESTAMP). return the day of the month (1-31).
   */
  private DefaultFunctionResolver dayOfMonth() {
    return define(BuiltinFunctionName.DAYOFMONTH.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, STRING)
    );
  }

  /**
   * DAYOFWEEK(STRING/DATE/DATETIME/TIMESTAMP).
   * return the weekday index for date (1 = Sunday, 2 = Monday, …, 7 = Saturday).
   */
  private DefaultFunctionResolver dayOfWeek() {
    return define(BuiltinFunctionName.DAYOFWEEK.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfWeek), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfWeek), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfWeek), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfWeek), INTEGER, STRING)
    );
  }

  /**
   * DAYOFYEAR(STRING/DATE/DATETIME/TIMESTAMP).
   * return the day of the year for date (1-366).
   */
  private DefaultFunctionResolver dayOfYear() {
    return define(BuiltinFunctionName.DAYOFYEAR.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfYear), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfYear), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfYear), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfYear), INTEGER, STRING)
    );
  }

  /**
   * FROM_DAYS(LONG). return the date value given the day number N.
   */
  private DefaultFunctionResolver from_days() {
    return define(BuiltinFunctionName.FROM_DAYS.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprFromDays), DATE, LONG));
  }

  private FunctionResolver from_unixtime() {
    return define(BuiltinFunctionName.FROM_UNIXTIME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprFromUnixTime), DATETIME, DOUBLE),
        impl(nullMissingHandling(DateTimeFunction::exprFromUnixTimeFormat),
            STRING, DOUBLE, STRING));
  }

  /**
   * HOUR(STRING/TIME/DATETIME/TIMESTAMP). return the hour value for time.
   */
  private DefaultFunctionResolver hour() {
    return define(BuiltinFunctionName.HOUR.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprHour), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprHour), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprHour), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprHour), INTEGER, TIMESTAMP)
    );
  }

  private FunctionResolver makedate() {
    return define(BuiltinFunctionName.MAKEDATE.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprMakeDate), DATE, DOUBLE, DOUBLE));
  }

  private FunctionResolver maketime() {
    return define(BuiltinFunctionName.MAKETIME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprMakeTime), TIME, DOUBLE, DOUBLE, DOUBLE));
  }

  /**
   * MICROSECOND(STRING/TIME/DATETIME/TIMESTAMP). return the microsecond value for time.
   */
  private DefaultFunctionResolver microsecond() {
    return define(BuiltinFunctionName.MICROSECOND.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprMicrosecond), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprMicrosecond), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprMicrosecond), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprMicrosecond), INTEGER, TIMESTAMP)
    );
  }

  /**
   * MINUTE(STRING/TIME/DATETIME/TIMESTAMP). return the minute value for time.
   */
  private DefaultFunctionResolver minute() {
    return define(BuiltinFunctionName.MINUTE.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprMinute), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprMinute), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprMinute), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprMinute), INTEGER, TIMESTAMP)
    );
  }

  /**
   * MONTH(STRING/DATE/DATETIME/TIMESTAMP). return the month for date (1-12).
   */
  private DefaultFunctionResolver month() {
    return define(BuiltinFunctionName.MONTH.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprMonth), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprMonth), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprMonth), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprMonth), INTEGER, STRING)
    );
  }

  /**
   * MONTHNAME(STRING/DATE/DATETIME/TIMESTAMP). return the full name of the month for date.
   */
  private DefaultFunctionResolver monthName() {
    return define(BuiltinFunctionName.MONTHNAME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprMonthName), STRING, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprMonthName), STRING, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprMonthName), STRING, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprMonthName), STRING, STRING)
    );
  }

  /**
   * QUARTER(STRING/DATE/DATETIME/TIMESTAMP). return the month for date (1-4).
   */
  private DefaultFunctionResolver quarter() {
    return define(BuiltinFunctionName.QUARTER.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprQuarter), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprQuarter), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprQuarter), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprQuarter), INTEGER, STRING)
    );
  }

  /**
   * SECOND(STRING/TIME/DATETIME/TIMESTAMP). return the second value for time.
   */
  private DefaultFunctionResolver second() {
    return define(BuiltinFunctionName.SECOND.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprSecond), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprSecond), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprSecond), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprSecond), INTEGER, TIMESTAMP)
    );
  }

  private DefaultFunctionResolver subdate() {
    return sub_date(BuiltinFunctionName.SUBDATE.getName());
  }

  /**
   * Extracts the time part of a date and time value.
   * Also to construct a time type. The supported signatures:
   * STRING/DATE/DATETIME/TIME/TIMESTAMP -> TIME
   */
  private DefaultFunctionResolver time() {
    return define(BuiltinFunctionName.TIME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, TIMESTAMP));
  }

  /**
   * TIME_TO_SEC(STRING/TIME/DATETIME/TIMESTAMP). return the time argument, converted to seconds.
   */
  private DefaultFunctionResolver time_to_sec() {
    return define(BuiltinFunctionName.TIME_TO_SEC.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprTimeToSec), LONG, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprTimeToSec), LONG, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprTimeToSec), LONG, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprTimeToSec), LONG, DATETIME)
    );
  }

  /**
   * Extracts the timestamp of a date and time value.
   * Also to construct a date type. The supported signatures:
   * STRING/DATE/DATETIME/TIMESTAMP -> DATE
   */
  private DefaultFunctionResolver timestamp() {
    return define(BuiltinFunctionName.TIMESTAMP.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, TIMESTAMP));
  }

  /**
   * TO_DAYS(STRING/DATE/DATETIME/TIMESTAMP). return the day number of the given date.
   */
  private DefaultFunctionResolver to_days() {
    return define(BuiltinFunctionName.TO_DAYS.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprToDays), LONG, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprToDays), LONG, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprToDays), LONG, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprToDays), LONG, DATETIME));
  }

  private FunctionResolver unix_timestamp() {
    return define(BuiltinFunctionName.UNIX_TIMESTAMP.getName(),
        impl(DateTimeFunction::unixTimeStamp, LONG),
        impl(nullMissingHandling(DateTimeFunction::unixTimeStampOf), DOUBLE, DATE),
        impl(nullMissingHandling(DateTimeFunction::unixTimeStampOf), DOUBLE, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::unixTimeStampOf), DOUBLE, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::unixTimeStampOf), DOUBLE, DOUBLE)
    );
  }

  /**
   * WEEK(DATE[,mode]). return the week number for date.
   */
  private DefaultFunctionResolver week() {
    return define(BuiltinFunctionName.WEEK.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprWeekWithoutMode), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprWeekWithoutMode), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprWeekWithoutMode), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprWeekWithoutMode), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprWeek), INTEGER, DATE, INTEGER),
        impl(nullMissingHandling(DateTimeFunction::exprWeek), INTEGER, DATETIME, INTEGER),
        impl(nullMissingHandling(DateTimeFunction::exprWeek), INTEGER, TIMESTAMP, INTEGER),
        impl(nullMissingHandling(DateTimeFunction::exprWeek), INTEGER, STRING, INTEGER)
    );
  }

  /**
   * YEAR(STRING/DATE/DATETIME/TIMESTAMP). return the year for date (1000-9999).
   */
  private DefaultFunctionResolver year() {
    return define(BuiltinFunctionName.YEAR.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprYear), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprYear), INTEGER, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprYear), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunction::exprYear), INTEGER, STRING)
    );
  }

  /**
   * Formats date according to format specifier. First argument is date, second is format.
   * Detailed supported signatures:
   * (STRING, STRING) -> STRING
   * (DATE, STRING) -> STRING
   * (DATETIME, STRING) -> STRING
   * (TIMESTAMP, STRING) -> STRING
   */
  private DefaultFunctionResolver date_format() {
    return define(BuiltinFunctionName.DATE_FORMAT.getName(),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedDate),
            STRING, STRING, STRING),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedDate),
            STRING, DATE, STRING),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedDate),
            STRING, DATETIME, STRING),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedDate),
            STRING, TIMESTAMP, STRING)
    );
  }

  /**
   * ADDDATE function implementation for ExprValue.
   *
   * @param date ExprValue of String/Date/Datetime/Timestamp type.
   * @param expr ExprValue of Interval type, the temporal amount to add.
   * @return Datetime resulted from expr added to date.
   */
  private ExprValue exprAddDateInterval(ExprValue date, ExprValue expr) {
    ExprValue exprValue = new ExprDatetimeValue(date.datetimeValue().plus(expr.intervalValue()));
    return (exprValue.timeValue().toSecondOfDay() == 0 ? new ExprDateValue(exprValue.dateValue())
        : exprValue);
  }

  /**
   * ADDDATE function implementation for ExprValue.
   *
   * @param date ExprValue of String/Date/Datetime/Timestamp type.
   * @param days ExprValue of Long type, representing the number of days to add.
   * @return Date/Datetime resulted from days added to date.
   */
  private ExprValue exprAddDateDays(ExprValue date, ExprValue days) {
    ExprValue exprValue = new ExprDatetimeValue(date.datetimeValue().plusDays(days.longValue()));
    return (exprValue.timeValue().toSecondOfDay() == 0 ? new ExprDateValue(exprValue.dateValue())
        : exprValue);
  }

  /**
   * CONVERT_TZ function implementation for ExprValue.
   * Returns null for time zones outside of +13:00 and -12:00.
   *
   * @param startingDateTime ExprValue of DateTime that is being converted from
   * @param fromTz           ExprValue of time zone, representing the time to convert from.
   * @param toTz             ExprValue of time zone, representing the time to convert to.
   * @return DateTime that has been converted to the to_tz timezone.
   */
  private ExprValue exprConvertTZ(ExprValue startingDateTime, ExprValue fromTz, ExprValue toTz) {
    if (startingDateTime.type() == ExprCoreType.STRING) {
      startingDateTime = exprDateTimeNoTimezone(startingDateTime);
    }
    try {
      ZoneId convertedFromTz = ZoneId.of(fromTz.stringValue());
      ZoneId convertedToTz = ZoneId.of(toTz.stringValue());

      // isValidMySqlTimeZoneId checks if the timezone is within the range accepted by
      // MySQL standard.
      if (!DateTimeUtils.isValidMySqlTimeZoneId(convertedFromTz)
          || !DateTimeUtils.isValidMySqlTimeZoneId(convertedToTz)) {
        return ExprNullValue.of();
      }
      ZonedDateTime zonedDateTime =
          startingDateTime.datetimeValue().atZone(convertedFromTz);
      return new ExprDatetimeValue(
          zonedDateTime.withZoneSameInstant(convertedToTz).toLocalDateTime());


      // Catches exception for invalid timezones.
      // ex. "+0:00" is an invalid timezone and would result in this exception being thrown.
    } catch (ExpressionEvaluationException | DateTimeException e) {
      return ExprNullValue.of();
    }
  }

  /**
   * Date implementation for ExprValue.
   *
   * @param exprValue ExprValue of Date type or String type.
   * @return ExprValue.
   */
  private ExprValue exprDate(ExprValue exprValue) {
    if (exprValue instanceof ExprStringValue) {
      return new ExprDateValue(exprValue.stringValue());
    } else {
      return new ExprDateValue(exprValue.dateValue());
    }
  }

  /**
   * DateTime implementation for ExprValue.
   *
   * @param dateTime ExprValue of String type.
   * @param timeZone ExprValue of String type (or null).
   * @return ExprValue of date type.
   */
  private ExprValue exprDateTime(ExprValue dateTime, ExprValue timeZone) {
    String defaultTimeZone = TimeZone.getDefault().getID();


    try {
      LocalDateTime ldtFormatted =
          LocalDateTime.parse(dateTime.stringValue(), DATE_TIME_FORMATTER_STRICT_WITH_TZ);
      if (timeZone.isNull()) {
        return new ExprDatetimeValue(ldtFormatted);
      }

      // Used if datetime field is invalid format.
    } catch (DateTimeParseException e) {
      return ExprNullValue.of();
    }

    ExprValue convertTZResult;
    ExprDatetimeValue ldt;
    String toTz;

    try {
      ZonedDateTime zdtWithZoneOffset =
          ZonedDateTime.parse(dateTime.stringValue(), DATE_TIME_FORMATTER_STRICT_WITH_TZ);
      ZoneId fromTZ = zdtWithZoneOffset.getZone();

      ldt = new ExprDatetimeValue(zdtWithZoneOffset.toLocalDateTime());
      toTz = String.valueOf(fromTZ);
    } catch (DateTimeParseException e) {
      ldt = new ExprDatetimeValue(dateTime.stringValue());
      toTz = defaultTimeZone;
    }
    convertTZResult = exprConvertTZ(
        ldt,
        new ExprStringValue(toTz),
        timeZone);

    return convertTZResult;
  }

  /**
   * DateTime implementation for ExprValue without a timezone to convert to.
   *
   * @param dateTime ExprValue of String type.
   * @return ExprValue of date type.
   */
  private ExprValue exprDateTimeNoTimezone(ExprValue dateTime) {
    return exprDateTime(dateTime, ExprNullValue.of());
  }

  /**
   * Name of the Weekday implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprDayName(ExprValue date) {
    return new ExprStringValue(
        date.dateValue().getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.getDefault()));
  }

  /**
   * Day of Month implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprDayOfMonth(ExprValue date) {
    return new ExprIntegerValue(date.dateValue().getDayOfMonth());
  }

  /**
   * Day of Week implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprDayOfWeek(ExprValue date) {
    return new ExprIntegerValue((date.dateValue().getDayOfWeek().getValue() % 7) + 1);
  }

  /**
   * Day of Year implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprDayOfYear(ExprValue date) {
    return new ExprIntegerValue(date.dateValue().getDayOfYear());
  }

  /**
   * From_days implementation for ExprValue.
   *
   * @param exprValue Day number N.
   * @return ExprValue.
   */
  private ExprValue exprFromDays(ExprValue exprValue) {
    return new ExprDateValue(LocalDate.ofEpochDay(exprValue.longValue() - DAYS_0000_TO_1970));
  }

  private ExprValue exprFromUnixTime(ExprValue time) {
    if (0 > time.doubleValue()) {
      return ExprNullValue.of();
    }
    // According to MySQL documentation:
    //     effective maximum is 32536771199.999999, which returns '3001-01-18 23:59:59.999999' UTC.
    //     Regardless of platform or version, a greater value for first argument than the effective
    //     maximum returns 0.
    if (MYSQL_MAX_TIMESTAMP <= time.doubleValue()) {
      return ExprNullValue.of();
    }
    return new ExprDatetimeValue(exprFromUnixTimeImpl(time));
  }

  private LocalDateTime exprFromUnixTimeImpl(ExprValue time) {
    return LocalDateTime.ofInstant(
            Instant.ofEpochSecond((long)Math.floor(time.doubleValue())),
            ZoneId.of("UTC"))
        .withNano((int)((time.doubleValue() % 1) * 1E9));
  }

  private ExprValue exprFromUnixTimeFormat(ExprValue time, ExprValue format) {
    var value = exprFromUnixTime(time);
    if (value.equals(ExprNullValue.of())) {
      return ExprNullValue.of();
    }
    return DateTimeFormatterUtil.getFormattedDate(value, format);
  }

  /**
   * Hour implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprHour(ExprValue time) {
    return new ExprIntegerValue(time.timeValue().getHour());
  }

  /**
   * Following MySQL, function receives arguments of type double and rounds them before use.
   * Furthermore:
   *  - zero year interpreted as 2000
   *  - negative year is not accepted
   *  - @dayOfYear should be greater than 1
   *  - if @dayOfYear is greater than 365/366, calculation goes to the next year(s)
   *
   * @param yearExpr year
   * @param dayOfYearExp day of the @year, starting from 1
   * @return Date - ExprDateValue object with LocalDate
   */
  private ExprValue exprMakeDate(ExprValue yearExpr, ExprValue dayOfYearExp) {
    var year = Math.round(yearExpr.doubleValue());
    var dayOfYear = Math.round(dayOfYearExp.doubleValue());
    // We need to do this to comply with MySQL
    if (0 >= dayOfYear || 0 > year) {
      return ExprNullValue.of();
    }
    if (0 == year) {
      year = 2000;
    }
    return new ExprDateValue(LocalDate.ofYearDay((int)year, 1).plusDays(dayOfYear - 1));
  }

  /**
   * Following MySQL, function receives arguments of type double. @hour and @minute are rounded,
   * while @second used as is, including fraction part.
   * @param hourExpr hour
   * @param minuteExpr minute
   * @param secondExpr second
   * @return Time - ExprTimeValue object with LocalTime
   */
  private ExprValue exprMakeTime(ExprValue hourExpr, ExprValue minuteExpr, ExprValue secondExpr) {
    var hour = Math.round(hourExpr.doubleValue());
    var minute = Math.round(minuteExpr.doubleValue());
    var second = secondExpr.doubleValue();
    if (0 > hour || 0 > minute || 0 > second) {
      return ExprNullValue.of();
    }
    return new ExprTimeValue(LocalTime.parse(String.format("%02d:%02d:%012.9f",
        hour, minute, second), DateTimeFormatter.ISO_TIME));
  }

  /**
   * Microsecond implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprMicrosecond(ExprValue time) {
    return new ExprIntegerValue(
        TimeUnit.MICROSECONDS.convert(time.timeValue().getNano(), TimeUnit.NANOSECONDS));
  }

  /**
   * Minute implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprMinute(ExprValue time) {
    return new ExprIntegerValue(time.timeValue().getMinute());
  }

  /**
   * Month for date implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprMonth(ExprValue date) {
    return new ExprIntegerValue(date.dateValue().getMonthValue());
  }

  /**
   * Name of the Month implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprMonthName(ExprValue date) {
    return new ExprStringValue(
        date.dateValue().getMonth().getDisplayName(TextStyle.FULL, Locale.getDefault()));
  }

  /**
   * Quarter for date implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprQuarter(ExprValue date) {
    int month = date.dateValue().getMonthValue();
    return new ExprIntegerValue((month / 3) + ((month % 3) == 0 ? 0 : 1));
  }

  /**
   * Second implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprSecond(ExprValue time) {
    return new ExprIntegerValue(time.timeValue().getSecond());
  }

  /**
   * SUBDATE function implementation for ExprValue.
   *
   * @param date ExprValue of String/Date/Datetime/Timestamp type.
   * @param days ExprValue of Long type, representing the number of days to subtract.
   * @return Date/Datetime resulted from days subtracted to date.
   */
  private ExprValue exprSubDateDays(ExprValue date, ExprValue days) {
    ExprValue exprValue = new ExprDatetimeValue(date.datetimeValue().minusDays(days.longValue()));
    return (exprValue.timeValue().toSecondOfDay() == 0 ? new ExprDateValue(exprValue.dateValue())
        : exprValue);
  }

  /**
   * SUBDATE function implementation for ExprValue.
   *
   * @param date ExprValue of String/Date/Datetime/Timestamp type.
   * @param expr ExprValue of Interval type, the temporal amount to subtract.
   * @return Datetime resulted from expr subtracted to date.
   */
  private ExprValue exprSubDateInterval(ExprValue date, ExprValue expr) {
    ExprValue exprValue = new ExprDatetimeValue(date.datetimeValue().minus(expr.intervalValue()));
    return (exprValue.timeValue().toSecondOfDay() == 0 ? new ExprDateValue(exprValue.dateValue())
        : exprValue);
  }

  /**
   * Time implementation for ExprValue.
   *
   * @param exprValue ExprValue of Time type or String.
   * @return ExprValue.
   */
  private ExprValue exprTime(ExprValue exprValue) {
    if (exprValue instanceof ExprStringValue) {
      return new ExprTimeValue(exprValue.stringValue());
    } else {
      return new ExprTimeValue(exprValue.timeValue());
    }
  }

  /**
   * Timestamp implementation for ExprValue.
   *
   * @param exprValue ExprValue of Timestamp type or String type.
   * @return ExprValue.
   */
  private ExprValue exprTimestamp(ExprValue exprValue) {
    if (exprValue instanceof ExprStringValue) {
      return new ExprTimestampValue(exprValue.stringValue());
    } else {
      return new ExprTimestampValue(exprValue.timestampValue());
    }
  }

  /**
   * Time To Sec implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprTimeToSec(ExprValue time) {
    return new ExprLongValue(time.timeValue().toSecondOfDay());
  }

  /**
   * To_days implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprToDays(ExprValue date) {
    return new ExprLongValue(date.dateValue().toEpochDay() + DAYS_0000_TO_1970);
  }

  /**
   * Week for date implementation for ExprValue.
   *
   * @param date ExprValue of Date/Datetime/Timestamp/String type.
   * @param mode ExprValue of Integer type.
   */
  private ExprValue exprWeek(ExprValue date, ExprValue mode) {
    return new ExprIntegerValue(
        CalendarLookup.getWeekNumber(mode.integerValue(), date.dateValue()));
  }

  private ExprValue unixTimeStamp() {
    return new ExprLongValue(Instant.now().getEpochSecond());
  }

  private ExprValue unixTimeStampOf(ExprValue value) {
    var res = unixTimeStampOfImpl(value);
    if (res == null) {
      return ExprNullValue.of();
    }
    if (res < 0) {
      // According to MySQL returns 0 if year < 1970, don't return negative values as java does.
      return new ExprDoubleValue(0);
    }
    if (res >= MYSQL_MAX_TIMESTAMP) {
      // Return 0 also for dates > '3001-01-19 03:14:07.999999' UTC (32536771199.999999 sec)
      return new ExprDoubleValue(0);
    }
    return new ExprDoubleValue(res);
  }

  private Double unixTimeStampOfImpl(ExprValue value) {
    // Also, according to MySQL documentation:
    //    The date argument may be a DATE, DATETIME, or TIMESTAMP ...
    switch ((ExprCoreType)value.type()) {
      case DATE: return value.dateValue().toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) + 0d;
      case DATETIME: return value.datetimeValue().toEpochSecond(ZoneOffset.UTC)
          + value.datetimeValue().getNano() / 1E9;
      case TIMESTAMP: return value.timestampValue().getEpochSecond()
          + value.timestampValue().getNano() / 1E9;
      default:
        //     ... or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format.
        //     If the argument includes a time part, it may optionally include a fractional
        //     seconds part.

        var format = new DecimalFormat("0.#");
        format.setMinimumFractionDigits(0);
        format.setMaximumFractionDigits(6);
        String input = format.format(value.doubleValue());
        double fraction = 0;
        if (input.contains(".")) {
          // Keeping fraction second part and adding it to the result, don't parse it
          // Because `toEpochSecond` returns only `long`
          // input = 12345.6789 becomes input = 12345 and fraction = 0.6789
          fraction = value.doubleValue() - Math.round(Math.ceil(value.doubleValue()));
          input = input.substring(0, input.indexOf('.'));
        }
        try {
          var res = LocalDateTime.parse(input, DATE_TIME_FORMATTER_SHORT_YEAR);
          return res.toEpochSecond(ZoneOffset.UTC) + fraction;
        } catch (DateTimeParseException ignored) {
          // nothing to do, try another format
        }
        try {
          var res = LocalDateTime.parse(input, DATE_TIME_FORMATTER_LONG_YEAR);
          return res.toEpochSecond(ZoneOffset.UTC) + fraction;
        } catch (DateTimeParseException ignored) {
          // nothing to do, try another format
        }
        try {
          var res = LocalDate.parse(input, DATE_FORMATTER_SHORT_YEAR);
          return res.toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) + 0d;
        } catch (DateTimeParseException ignored) {
          // nothing to do, try another format
        }
        try {
          var res = LocalDate.parse(input, DATE_FORMATTER_LONG_YEAR);
          return res.toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) + 0d;
        } catch (DateTimeParseException ignored) {
          return null;
        }
    }
  }

  /**
   * Week for date implementation for ExprValue.
   * When mode is not specified default value mode 0 is used for default_week_format.
   *
   * @param date ExprValue of Date/Datetime/Timestamp/String type.
   * @return ExprValue.
   */
  private ExprValue exprWeekWithoutMode(ExprValue date) {
    return exprWeek(date, new ExprIntegerValue(0));
  }

  /**
   * Year for date implementation for ExprValue.
   *
   * @param date ExprValue of Date/String type.
   * @return ExprValue.
   */
  private ExprValue exprYear(ExprValue date) {
    return new ExprIntegerValue(date.dateValue().getYear());
  }

  private LocalDateTime formatNow(Clock clock) {
    return formatNow(clock, 0);
  }

  /**
   * Prepare LocalDateTime value. Truncate fractional second part according to the argument.
   * @param fsp argument is given to specify a fractional seconds precision from 0 to 6,
   *            the return value includes a fractional seconds part of that many digits.
   * @return LocalDateTime object.
   */
  private LocalDateTime formatNow(Clock clock,  Integer fsp) {
    var res = LocalDateTime.now(clock);
    var defaultPrecision = 9; // There are 10^9 nanoseconds in one second
    if (fsp < 0 || fsp > 6) { // Check that the argument is in the allowed range [0, 6]
      throw new IllegalArgumentException(
          String.format("Invalid `fsp` value: %d, allowed 0 to 6", fsp));
    }
    var nano = new BigDecimal(res.getNano())
        .setScale(fsp - defaultPrecision, RoundingMode.DOWN).intValue();
    return res.withNano(nano);
  }
}
