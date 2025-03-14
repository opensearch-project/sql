/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.implWithProperties;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandlingWithProperties;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_LONG_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_NO_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_SHORT_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_SINGLE_DIGIT_MONTH;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_FORMATTER_SINGLE_DIGIT_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_LONG_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_SHORT_YEAR;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_STRICT_WITH_TZ;
import static org.opensearch.sql.utils.DateTimeFormatters.FULL_DATE_LENGTH;
import static org.opensearch.sql.utils.DateTimeFormatters.NO_YEAR_DATE_LENGTH;
import static org.opensearch.sql.utils.DateTimeFormatters.SHORT_DATE_LENGTH;
import static org.opensearch.sql.utils.DateTimeFormatters.SINGLE_DIGIT_MONTH_DATE_LENGTH;
import static org.opensearch.sql.utils.DateTimeFormatters.SINGLE_DIGIT_YEAR_DATE_LENGTH;
import static org.opensearch.sql.utils.DateTimeUtils.extractDate;
import static org.opensearch.sql.utils.DateTimeUtils.extractTimestamp;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
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
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprDateValue;
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
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionDSL;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableFunction;
import org.opensearch.sql.expression.function.SerializableTriFunction;
import org.opensearch.sql.utils.DateTimeUtils;

/**
 * The definition of date and time functions. 1) have the clear interface for function define. 2)
 * the implementation should rely on ExprValue.
 */
@UtilityClass
@SuppressWarnings("unchecked")
public class DateTimeFunctions {
  // The number of seconds per day
  public static final long SECONDS_PER_DAY = 86400;

  // The number of days from year zero to year 1970.
  private static final Long DAYS_0000_TO_1970 = (146097 * 5L) - (30L * 365L + 7L);

  // MySQL doesn't process any timestamp values which are greater than
  // 32536771199.999999, or equivalent '3001-01-18 23:59:59.999999' UTC
  private static final Double MYSQL_MAX_TIMESTAMP = 32536771200d;

  // Mode used for week/week_of_year function by default when no argument is provided
  private static final ExprIntegerValue DEFAULT_WEEK_OF_YEAR_MODE = new ExprIntegerValue(0);

  // Map used to determine format output for the extract function
  private static final Map<String, String> extract_formats =
      ImmutableMap.<String, String>builder()
          .put("MICROSECOND", "SSSSSS")
          .put("SECOND", "ss")
          .put("MINUTE", "mm")
          .put("HOUR", "HH")
          .put("DAY", "dd")
          .put("WEEK", "w")
          .put("MONTH", "MM")
          .put("YEAR", "yyyy")
          .put("SECOND_MICROSECOND", "ssSSSSSS")
          .put("MINUTE_MICROSECOND", "mmssSSSSSS")
          .put("MINUTE_SECOND", "mmss")
          .put("HOUR_MICROSECOND", "HHmmssSSSSSS")
          .put("HOUR_SECOND", "HHmmss")
          .put("HOUR_MINUTE", "HHmm")
          .put("DAY_MICROSECOND", "ddHHmmssSSSSSS")
          .put("DAY_SECOND", "ddHHmmss")
          .put("DAY_MINUTE", "ddHHmm")
          .put("DAY_HOUR", "ddHH")
          .put("YEAR_MONTH", "yyyyMM")
          .put("QUARTER", "Q")
          .build();

  // Map used to determine format output for the get_format function
  private static final Table<String, String, String> formats =
      ImmutableTable.<String, String, String>builder()
          .put("date", "usa", "%m.%d.%Y")
          .put("date", "jis", "%Y-%m-%d")
          .put("date", "iso", "%Y-%m-%d")
          .put("date", "eur", "%d.%m.%Y")
          .put("date", "internal", "%Y%m%d")
          .put("time", "usa", "%h:%i:%s %p")
          .put("time", "jis", "%H:%i:%s")
          .put("time", "iso", "%H:%i:%s")
          .put("time", "eur", "%H.%i.%s")
          .put("time", "internal", "%H%i%s")
          .put("timestamp", "usa", "%Y-%m-%d %H.%i.%s")
          .put("timestamp", "jis", "%Y-%m-%d %H:%i:%s")
          .put("timestamp", "iso", "%Y-%m-%d %H:%i:%s")
          .put("timestamp", "eur", "%Y-%m-%d %H.%i.%s")
          .put("timestamp", "internal", "%Y%m%d%H%i%s")
          .build();

  /**
   * Register Date and Time Functions.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(adddate());
    repository.register(addtime());
    repository.register(convert_tz());
    repository.register(curtime());
    repository.register(curdate());
    repository.register(current_date());
    repository.register(current_time());
    repository.register(current_timestamp());
    repository.register(date());
    repository.register(datediff());
    repository.register(datetime());
    repository.register(date_add());
    repository.register(date_format());
    repository.register(date_sub());
    repository.register(day());
    repository.register(dayName());
    repository.register(dayOfMonth(BuiltinFunctionName.DAYOFMONTH));
    repository.register(dayOfMonth(BuiltinFunctionName.DAY_OF_MONTH));
    repository.register(dayOfWeek(BuiltinFunctionName.DAYOFWEEK.getName()));
    repository.register(dayOfWeek(BuiltinFunctionName.DAY_OF_WEEK.getName()));
    repository.register(dayOfYear(BuiltinFunctionName.DAYOFYEAR));
    repository.register(dayOfYear(BuiltinFunctionName.DAY_OF_YEAR));
    repository.register(extract());
    repository.register(from_days());
    repository.register(from_unixtime());
    repository.register(get_format());
    repository.register(hour(BuiltinFunctionName.HOUR));
    repository.register(hour(BuiltinFunctionName.HOUR_OF_DAY));
    repository.register(last_day());
    repository.register(localtime());
    repository.register(localtimestamp());
    repository.register(makedate());
    repository.register(maketime());
    repository.register(microsecond());
    repository.register(minute(BuiltinFunctionName.MINUTE));
    repository.register(minute_of_day());
    repository.register(minute(BuiltinFunctionName.MINUTE_OF_HOUR));
    repository.register(month(BuiltinFunctionName.MONTH));
    repository.register(month(BuiltinFunctionName.MONTH_OF_YEAR));
    repository.register(monthName());
    repository.register(now());
    repository.register(period_add());
    repository.register(period_diff());
    repository.register(quarter());
    repository.register(sec_to_time());
    repository.register(second(BuiltinFunctionName.SECOND));
    repository.register(second(BuiltinFunctionName.SECOND_OF_MINUTE));
    repository.register(subdate());
    repository.register(subtime());
    repository.register(str_to_date());
    repository.register(sysdate());
    repository.register(time());
    repository.register(time_format());
    repository.register(time_to_sec());
    repository.register(timediff());
    repository.register(timestamp());
    repository.register(timestampadd());
    repository.register(timestampdiff());
    repository.register(to_days());
    repository.register(to_seconds());
    repository.register(unix_timestamp());
    repository.register(utc_date());
    repository.register(utc_time());
    repository.register(utc_timestamp());
    repository.register(week(BuiltinFunctionName.WEEK));
    repository.register(week(BuiltinFunctionName.WEEKOFYEAR));
    repository.register(week(BuiltinFunctionName.WEEK_OF_YEAR));
    repository.register(weekday());
    repository.register(year());
    repository.register(yearweek());
  }

  /**
   * NOW() returns a constant time that indicates the time at which the statement began to execute.
   * `fsp` argument support is removed until refactoring to avoid bug where `now()`, `now(x)` and
   * `now(y) return different values.
   */
  private FunctionResolver now(FunctionName functionName) {
    return define(
        functionName,
        implWithProperties(
            functionProperties ->
                new ExprTimestampValue(formatNow(functionProperties.getQueryStartClock())),
            TIMESTAMP));
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

  /** SYSDATE() returns the time at which it executes. */
  private FunctionResolver sysdate() {
    return define(
        BuiltinFunctionName.SYSDATE.getName(),
        implWithProperties(
            functionProperties -> new ExprTimestampValue(formatNow(Clock.systemDefaultZone())),
            TIMESTAMP),
        FunctionDSL.implWithProperties(
            (functionProperties, v) ->
                new ExprTimestampValue(formatNow(Clock.systemDefaultZone(), v.integerValue())),
            TIMESTAMP,
            INTEGER));
  }

  /** Synonym for @see `now`. */
  private FunctionResolver curtime(FunctionName functionName) {
    return define(
        functionName,
        implWithProperties(
            functionProperties ->
                new ExprTimeValue(formatNow(functionProperties.getQueryStartClock()).toLocalTime()),
            TIME));
  }

  private FunctionResolver curtime() {
    return curtime(BuiltinFunctionName.CURTIME.getName());
  }

  private FunctionResolver current_time() {
    return curtime(BuiltinFunctionName.CURRENT_TIME.getName());
  }

  private FunctionResolver curdate(FunctionName functionName) {
    return define(
        functionName,
        implWithProperties(
            functionProperties ->
                new ExprDateValue(formatNow(functionProperties.getQueryStartClock()).toLocalDate()),
            DATE));
  }

  private FunctionResolver curdate() {
    return curdate(BuiltinFunctionName.CURDATE.getName());
  }

  private FunctionResolver current_date() {
    return curdate(BuiltinFunctionName.CURRENT_DATE.getName());
  }

  /**
   * A common signature for `date_add` and `date_sub`.<br>
   * Specify a start date and add/subtract a temporal amount to/from the date.<br>
   * The return type depends on the date type and the interval unit. Detailed supported signatures:
   * <br>
   * (DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP<br>
   * MySQL has these signatures too<br>
   * (DATE, INTERVAL) -> DATE // when interval has no time part<br>
   * (TIME, INTERVAL) -> TIME // when interval has no date part<br>
   * (STRING, INTERVAL) -> STRING // when argument has date or timestamp string,<br>
   * // result has date or timestamp depending on interval type<br>
   */
  private Stream<SerializableFunction<?, ?>> get_date_add_date_sub_signatures(
      SerializableTriFunction<FunctionProperties, ExprValue, ExprValue, ExprValue> function) {
    return Stream.of(
        implWithProperties(nullMissingHandlingWithProperties(function), TIMESTAMP, DATE, INTERVAL),
        implWithProperties(
            nullMissingHandlingWithProperties(function), TIMESTAMP, TIMESTAMP, INTERVAL),
        implWithProperties(nullMissingHandlingWithProperties(function), TIMESTAMP, TIME, INTERVAL));
  }

  /**
   * A common signature for `adddate` and `subdate`.<br>
   * Adds/subtracts an integer number of days to/from the first argument.<br>
   * (DATE, LONG) -> DATE<br>
   * (TIME/TIMESTAMP, LONG) -> TIMESTAMP
   */
  private Stream<SerializableFunction<?, ?>> get_adddate_subdate_signatures(
      SerializableTriFunction<FunctionProperties, ExprValue, ExprValue, ExprValue> function) {
    return Stream.of(
        implWithProperties(nullMissingHandlingWithProperties(function), DATE, DATE, LONG),
        implWithProperties(nullMissingHandlingWithProperties(function), TIMESTAMP, TIMESTAMP, LONG),
        implWithProperties(nullMissingHandlingWithProperties(function), TIMESTAMP, TIME, LONG));
  }

  private DefaultFunctionResolver adddate() {
    return define(
        BuiltinFunctionName.ADDDATE.getName(),
        (SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>[])
            (Stream.concat(
                    get_date_add_date_sub_signatures(DateTimeFunctions::exprAddDateInterval),
                    get_adddate_subdate_signatures(DateTimeFunctions::exprAddDateDays))
                .toArray(SerializableFunction<?, ?>[]::new)));
  }

  /**
   * Adds expr2 to expr1 and returns the result.<br>
   * (TIME, TIME/DATE/TIMESTAMP) -> TIME<br>
   * (DATE/TIMESTAMP, TIME/DATE/TIMESTAMP) -> TIMESTAMP<br>
   * TODO: MySQL has these signatures too<br>
   * (STRING, STRING/TIME) -> STRING // second arg - string with time only<br>
   * (x, STRING) -> NULL // second arg - string with timestamp<br>
   * (x, STRING/DATE) -> x // second arg - string with date only
   */
  private DefaultFunctionResolver addtime() {
    return define(
        BuiltinFunctionName.ADDTIME.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime), TIME, TIME, TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime), TIME, TIME, DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIME,
            TIME,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIMESTAMP,
            DATE,
            TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIMESTAMP,
            DATE,
            DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIMESTAMP,
            DATE,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIMESTAMP,
            TIMESTAMP,
            TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIMESTAMP,
            TIMESTAMP,
            DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprAddTime),
            TIMESTAMP,
            TIMESTAMP,
            TIMESTAMP));
  }

  /**
   * Converts date/time from a specified timezone to another specified timezone.<br>
   * The supported signatures:<br>
   * (TIMESTAMP, STRING, STRING) -> TIMESTAMP<br>
   * (STRING, STRING, STRING) -> TIMESTAMP
   */
  private DefaultFunctionResolver convert_tz() {
    return define(
        BuiltinFunctionName.CONVERT_TZ.getName(),
        impl(
            nullMissingHandling(DateTimeFunctions::exprConvertTZ),
            TIMESTAMP,
            TIMESTAMP,
            STRING,
            STRING),
        impl(
            nullMissingHandling(DateTimeFunctions::exprConvertTZ),
            TIMESTAMP,
            STRING,
            STRING,
            STRING));
  }

  /**
   * Extracts the date part of a date and time value. Also to construct a date type. The supported
   * signatures: STRING/DATE/TIMESTAMP -> DATE
   */
  private DefaultFunctionResolver date() {
    return define(
        BuiltinFunctionName.DATE.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprDate), DATE, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprDate), DATE, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprDate), DATE, TIMESTAMP));
  }

  /**
   * Calculates the difference of date part of given values.<br>
   * (DATE/TIMESTAMP/TIME, DATE/TIMESTAMP/TIME) -> LONG
   */
  private DefaultFunctionResolver datediff() {
    return define(
        BuiltinFunctionName.DATEDIFF.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff), LONG, DATE, DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff), LONG, DATE, TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff), LONG, TIME, DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff), LONG, TIME, TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff),
            LONG,
            TIMESTAMP,
            DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff),
            LONG,
            DATE,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff),
            LONG,
            TIMESTAMP,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff),
            LONG,
            TIMESTAMP,
            TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprDateDiff),
            LONG,
            TIME,
            TIMESTAMP));
  }

  /**
   * Specify a datetime with time zone field and a time zone to convert to.<br>
   * Returns a local datetime.<br>
   * (STRING, STRING) -> TIMESTAMP<br>
   * (STRING) -> TIMESTAMP
   */
  private FunctionResolver datetime() {
    return define(
        BuiltinFunctionName.DATETIME.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprDateTime), TIMESTAMP, STRING, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprDateTimeNoTimezone), TIMESTAMP, STRING));
  }

  private DefaultFunctionResolver date_add() {
    return define(
        BuiltinFunctionName.DATE_ADD.getName(),
        (SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>[])
            get_date_add_date_sub_signatures(DateTimeFunctions::exprAddDateInterval)
                .toArray(SerializableFunction<?, ?>[]::new));
  }

  private DefaultFunctionResolver date_sub() {
    return define(
        BuiltinFunctionName.DATE_SUB.getName(),
        (SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>[])
            get_date_add_date_sub_signatures(DateTimeFunctions::exprSubDateInterval)
                .toArray(SerializableFunction<?, ?>[]::new));
  }

  /** DAY(STRING/DATE/TIMESTAMP). return the day of the month (1-31). */
  private DefaultFunctionResolver day() {
    return define(
        BuiltinFunctionName.DAY.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfMonth), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfMonth), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfMonth), INTEGER, STRING));
  }

  /**
   * DAYNAME(STRING/DATE/TIMESTAMP). return the name of the weekday for date, including <br>
   * Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday.
   */
  private DefaultFunctionResolver dayName() {
    return define(
        BuiltinFunctionName.DAYNAME.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprDayName), STRING, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprDayName), STRING, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprDayName), STRING, STRING));
  }

  /** DAYOFMONTH(STRING/DATE/TIMESTAMP). return the day of the month (1-31). */
  private DefaultFunctionResolver dayOfMonth(BuiltinFunctionName name) {
    return define(
        name.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    DateTimeFunctions.dayOfMonthToday(functionProperties.getQueryStartClock())),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfMonth), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfMonth), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfMonth), INTEGER, TIMESTAMP));
  }

  /**
   * DAYOFWEEK(STRING/DATE/TIME/TIMESTAMP). return the weekday index for date (1 = Sunday, 2 =
   * Monday, ..., 7 = Saturday).
   */
  private DefaultFunctionResolver dayOfWeek(FunctionName name) {
    return define(
        name,
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    DateTimeFunctions.dayOfWeekToday(functionProperties.getQueryStartClock())),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfWeek), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfWeek), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfWeek), INTEGER, STRING));
  }

  /** DAYOFYEAR(STRING/DATE/TIMESTAMP). return the day of the year for date (1-366). */
  private DefaultFunctionResolver dayOfYear(BuiltinFunctionName dayOfYear) {
    return define(
        dayOfYear.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    DateTimeFunctions.dayOfYearToday(functionProperties.getQueryStartClock())),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfYear), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfYear), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprDayOfYear), INTEGER, STRING));
  }

  private DefaultFunctionResolver extract() {
    return define(
        BuiltinFunctionName.EXTRACT.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprExtractForTime),
            LONG,
            STRING,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprExtract), LONG, STRING, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprExtract), LONG, STRING, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprExtract), LONG, STRING, STRING));
  }

  /** FROM_DAYS(LONG). return the date value given the day number N. */
  private DefaultFunctionResolver from_days() {
    return define(
        BuiltinFunctionName.FROM_DAYS.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprFromDays), DATE, LONG));
  }

  private FunctionResolver from_unixtime() {
    return define(
        BuiltinFunctionName.FROM_UNIXTIME.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprFromUnixTime), TIMESTAMP, DOUBLE),
        impl(
            nullMissingHandling(DateTimeFunctions::exprFromUnixTimeFormat),
            STRING,
            DOUBLE,
            STRING));
  }

  private DefaultFunctionResolver get_format() {
    return define(
        BuiltinFunctionName.GET_FORMAT.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprGetFormat), STRING, STRING, STRING));
  }

  /** HOUR(STRING/TIME/DATE/TIMESTAMP). return the hour value for time. */
  private DefaultFunctionResolver hour(BuiltinFunctionName name) {
    return define(
        name.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprHour), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprHour), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprHour), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprHour), INTEGER, TIMESTAMP));
  }

  private DefaultFunctionResolver last_day() {
    return define(
        BuiltinFunctionName.LAST_DAY.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprLastDay), DATE, STRING),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    DateTimeFunctions.exprLastDayToday(functionProperties.getQueryStartClock())),
            DATE,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprLastDay), DATE, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprLastDay), DATE, TIMESTAMP));
  }

  private FunctionResolver makedate() {
    return define(
        BuiltinFunctionName.MAKEDATE.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprMakeDate), DATE, DOUBLE, DOUBLE));
  }

  private FunctionResolver maketime() {
    return define(
        BuiltinFunctionName.MAKETIME.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprMakeTime), TIME, DOUBLE, DOUBLE, DOUBLE));
  }

  /** MICROSECOND(STRING/TIME/TIMESTAMP). return the microsecond value for time. */
  private DefaultFunctionResolver microsecond() {
    return define(
        BuiltinFunctionName.MICROSECOND.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprMicrosecond), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprMicrosecond), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprMicrosecond), INTEGER, TIMESTAMP));
  }

  /** MINUTE(STRING/TIME/TIMESTAMP). return the minute value for time. */
  private DefaultFunctionResolver minute(BuiltinFunctionName name) {
    return define(
        name.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprMinute), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprMinute), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprMinute), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprMinute), INTEGER, TIMESTAMP));
  }

  /** MINUTE(STRING/TIME/TIMESTAMP). return the minute value for time. */
  private DefaultFunctionResolver minute_of_day() {
    return define(
        BuiltinFunctionName.MINUTE_OF_DAY.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprMinuteOfDay), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprMinuteOfDay), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprMinuteOfDay), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprMinuteOfDay), INTEGER, TIMESTAMP));
  }

  /** MONTH(STRING/DATE/TIMESTAMP). return the month for date (1-12). */
  private DefaultFunctionResolver month(BuiltinFunctionName month) {
    return define(
        month.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    DateTimeFunctions.monthOfYearToday(functionProperties.getQueryStartClock())),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprMonth), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprMonth), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprMonth), INTEGER, STRING));
  }

  /** MONTHNAME(STRING/DATE/TIMESTAMP). return the full name of the month for date. */
  private DefaultFunctionResolver monthName() {
    return define(
        BuiltinFunctionName.MONTHNAME.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprMonthName), STRING, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprMonthName), STRING, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprMonthName), STRING, STRING));
  }

  /**
   * Add N months to period P (in the format YYMM or YYYYMM). Returns a value in the format YYYYMM.
   * (INTEGER, INTEGER) -> INTEGER
   */
  private DefaultFunctionResolver period_add() {
    return define(
        BuiltinFunctionName.PERIOD_ADD.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprPeriodAdd), INTEGER, INTEGER, INTEGER));
  }

  /**
   * Returns the number of months between periods P1 and P2. P1 and P2 should be in the format YYMM
   * or YYYYMM.<br>
   * (INTEGER, INTEGER) -> INTEGER
   */
  private DefaultFunctionResolver period_diff() {
    return define(
        BuiltinFunctionName.PERIOD_DIFF.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprPeriodDiff), INTEGER, INTEGER, INTEGER));
  }

  /** QUARTER(STRING/DATE/TIMESTAMP). return the month for date (1-4). */
  private DefaultFunctionResolver quarter() {
    return define(
        BuiltinFunctionName.QUARTER.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprQuarter), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprQuarter), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprQuarter), INTEGER, STRING));
  }

  private DefaultFunctionResolver sec_to_time() {
    return define(
        BuiltinFunctionName.SEC_TO_TIME.getName(),
        impl((nullMissingHandling(DateTimeFunctions::exprSecToTime)), TIME, INTEGER),
        impl((nullMissingHandling(DateTimeFunctions::exprSecToTime)), TIME, LONG),
        impl((nullMissingHandling(DateTimeFunctions::exprSecToTimeWithNanos)), TIME, DOUBLE),
        impl((nullMissingHandling(DateTimeFunctions::exprSecToTimeWithNanos)), TIME, FLOAT));
  }

  /** SECOND(STRING/TIME/TIMESTAMP). return the second value for time. */
  private DefaultFunctionResolver second(BuiltinFunctionName name) {
    return define(
        name.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprSecond), INTEGER, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprSecond), INTEGER, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprSecond), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprSecond), INTEGER, TIMESTAMP));
  }

  private DefaultFunctionResolver subdate() {
    return define(
        BuiltinFunctionName.SUBDATE.getName(),
        (SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>[])
            (Stream.concat(
                    get_date_add_date_sub_signatures(DateTimeFunctions::exprSubDateInterval),
                    get_adddate_subdate_signatures(DateTimeFunctions::exprSubDateDays))
                .toArray(SerializableFunction<?, ?>[]::new)));
  }

  /**
   * Subtracts expr2 from expr1 and returns the result.<br>
   * (TIME, TIME/DATE/TIMESTAMP) -> TIME<br>
   * (DATE/TIMESTAMP, TIME/DATE/TIMESTAMP) -> TIMESTAMP<br>
   * TODO: MySQL has these signatures too<br>
   * (STRING, STRING/TIME) -> STRING // second arg - string with time only<br>
   * (x, STRING) -> NULL // second arg - string with timestamp<br>
   * (x, STRING/DATE) -> x // second arg - string with date only
   */
  private DefaultFunctionResolver subtime() {
    return define(
        BuiltinFunctionName.SUBTIME.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime), TIME, TIME, TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime), TIME, TIME, DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIME,
            TIME,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIMESTAMP,
            TIMESTAMP,
            TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIMESTAMP,
            TIMESTAMP,
            DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIMESTAMP,
            DATE,
            TIME),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIMESTAMP,
            DATE,
            DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIMESTAMP,
            DATE,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(DateTimeFunctions::exprSubTime),
            TIMESTAMP,
            TIMESTAMP,
            TIMESTAMP));
  }

  /**
   * Extracts a date, time, or timestamp from the given string. It accomplishes this using another
   * string which specifies the input format.
   */
  private DefaultFunctionResolver str_to_date() {
    return define(
        BuiltinFunctionName.STR_TO_DATE.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg, format) ->
                    DateTimeFunctions.exprStrToDate(functionProperties, arg, format)),
            TIMESTAMP,
            STRING,
            STRING));
  }

  /**
   * Extracts the time part of a date and time value. Also to construct a time type. The supported
   * signatures: STRING/DATE/TIME/TIMESTAMP -> TIME
   */
  private DefaultFunctionResolver time() {
    return define(
        BuiltinFunctionName.TIME.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprTime), TIME, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprTime), TIME, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprTime), TIME, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprTime), TIME, TIMESTAMP));
  }

  /**
   * Returns different between two times as a time.<br>
   * (TIME, TIME) -> TIME<br>
   * MySQL has these signatures too<br>
   * (DATE, DATE) -> TIME // result is > 24 hours<br>
   * (TIMESTAMP, TIMESTAMP) -> TIME // result is > 24 hours<br>
   * (x, x) -> NULL // when args have different types<br>
   * (STRING, STRING) -> TIME // argument strings contain same types only<br>
   * (STRING, STRING) -> NULL // argument strings are different types
   */
  private DefaultFunctionResolver timediff() {
    return define(
        BuiltinFunctionName.TIMEDIFF.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprTimeDiff), TIME, TIME, TIME));
  }

  /** TIME_TO_SEC(STRING/TIME/TIMESTAMP). return the time argument, converted to seconds. */
  private DefaultFunctionResolver time_to_sec() {
    return define(
        BuiltinFunctionName.TIME_TO_SEC.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprTimeToSec), LONG, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprTimeToSec), LONG, TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprTimeToSec), LONG, TIMESTAMP));
  }

  /**
   * Extracts the timestamp of a date and time value.<br>
   * Input strings may contain a timestamp only in format 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'<br>
   * STRING/DATE/TIME/TIMESTAMP -> TIMESTAMP<br>
   * STRING/DATE/TIME/TIMESTAMP, STRING/DATE/TIME/TIMESTAMP -> TIMESTAMP<br>
   * All types are converted to TIMESTAMP actually before the function call - it is responsibility
   * <br>
   * of the automatic cast mechanism defined in `ExprCoreType` and performed by `TypeCastOperator`.
   */
  private DefaultFunctionResolver timestamp() {
    return define(
        BuiltinFunctionName.TIMESTAMP.getName(),
        impl(nullMissingHandling(v -> v), TIMESTAMP, TIMESTAMP),
        // We can use FunctionProperties.None, because it is not used. It is required to convert
        // TIME to other datetime types, but arguments there are already converted.
        impl(
            nullMissingHandling((v1, v2) -> exprAddTime(FunctionProperties.None, v1, v2)),
            TIMESTAMP,
            TIMESTAMP,
            TIMESTAMP));
  }

  /**
   * Adds an interval of time to the provided DATE/TIME/TIMESTAMP/STRING argument. The interval of
   * time added is determined by the given first and second arguments. The first argument is an
   * interval type, and must be one of the tokens below... [MICROSECOND, SECOND, MINUTE, HOUR, DAY,
   * WEEK, MONTH, QUARTER, YEAR] The second argument is the amount of the interval type to be added.
   * The third argument is the DATE/TIME/TIMESTAMP/STRING to add to.
   *
   * @return The TIMESTAMP representing the summed DATE/TIME/TIMESTAMP and interval.
   */
  private DefaultFunctionResolver timestampadd() {
    return define(
        BuiltinFunctionName.TIMESTAMPADD.getName(),
        impl(
            nullMissingHandling(DateTimeFunctions::exprTimestampAdd),
            TIMESTAMP,
            STRING,
            INTEGER,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, part, amount, time) ->
                    exprTimestampAddForTimeType(
                        functionProperties.getQueryStartClock(), part, amount, time)),
            TIMESTAMP,
            STRING,
            INTEGER,
            TIME));
  }

  /**
   * Finds the difference between provided DATE/TIME/TIMESTAMP/STRING arguments. The first argument
   * is an interval type, and must be one of the tokens below... [MICROSECOND, SECOND, MINUTE, HOUR,
   * DAY, WEEK, MONTH, QUARTER, YEAR] The second argument the DATE/TIME/TIMESTAMP/STRING
   * representing the start time. The third argument is the DATE/TIME/TIMESTAMP/STRING representing
   * the end time.
   *
   * @return A LONG representing the difference between arguments, using the given interval type.
   */
  private DefaultFunctionResolver timestampdiff() {
    return define(
        BuiltinFunctionName.TIMESTAMPDIFF.getName(),
        impl(
            nullMissingHandling(DateTimeFunctions::exprTimestampDiff),
            TIMESTAMP,
            STRING,
            TIMESTAMP,
            TIMESTAMP),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, part, startTime, endTime) ->
                    exprTimestampDiffForTimeType(functionProperties, part, startTime, endTime)),
            TIMESTAMP,
            STRING,
            TIME,
            TIME));
  }

  /** TO_DAYS(STRING/DATE/TIMESTAMP). return the day number of the given date. */
  private DefaultFunctionResolver to_days() {
    return define(
        BuiltinFunctionName.TO_DAYS.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprToDays), LONG, STRING),
        impl(nullMissingHandling(DateTimeFunctions::exprToDays), LONG, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprToDays), LONG, DATE));
  }

  /**
   * TO_SECONDS(TIMESTAMP/LONG). return the seconds number of the given date. Arguments of type
   * STRING/TIMESTAMP/LONG are also accepted. STRING/TIMESTAMP/LONG arguments are automatically cast
   * to TIMESTAMP.
   */
  private DefaultFunctionResolver to_seconds() {
    return define(
        BuiltinFunctionName.TO_SECONDS.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprToSeconds), LONG, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprToSecondsForIntType), LONG, LONG));
  }

  private FunctionResolver unix_timestamp() {
    return define(
        BuiltinFunctionName.UNIX_TIMESTAMP.getName(),
        implWithProperties(
            functionProperties ->
                DateTimeFunctions.unixTimeStamp(functionProperties.getQueryStartClock()),
            LONG),
        impl(nullMissingHandling(DateTimeFunctions::unixTimeStampOf), DOUBLE, DATE),
        impl(nullMissingHandling(DateTimeFunctions::unixTimeStampOf), DOUBLE, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::unixTimeStampOf), DOUBLE, DOUBLE));
  }

  /** UTC_DATE(). return the current UTC Date in format yyyy-MM-dd */
  private DefaultFunctionResolver utc_date() {
    return define(
        BuiltinFunctionName.UTC_DATE.getName(),
        implWithProperties(functionProperties -> exprUtcDate(functionProperties), DATE));
  }

  /** UTC_TIME(). return the current UTC Time in format HH:mm:ss */
  private DefaultFunctionResolver utc_time() {
    return define(
        BuiltinFunctionName.UTC_TIME.getName(),
        implWithProperties(functionProperties -> exprUtcTime(functionProperties), TIME));
  }

  /** UTC_TIMESTAMP(). return the current UTC TimeStamp in format yyyy-MM-dd HH:mm:ss */
  private DefaultFunctionResolver utc_timestamp() {
    return define(
        BuiltinFunctionName.UTC_TIMESTAMP.getName(),
        implWithProperties(functionProperties -> exprUtcTimeStamp(functionProperties), TIMESTAMP));
  }

  /** WEEK(DATE[,mode]). return the week number for date. */
  private DefaultFunctionResolver week(BuiltinFunctionName week) {
    return define(
        week.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    DateTimeFunctions.weekOfYearToday(
                        DEFAULT_WEEK_OF_YEAR_MODE, functionProperties.getQueryStartClock())),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprWeekWithoutMode), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprWeekWithoutMode), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprWeekWithoutMode), INTEGER, STRING),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, time, modeArg) ->
                    DateTimeFunctions.weekOfYearToday(
                        modeArg, functionProperties.getQueryStartClock())),
            INTEGER,
            TIME,
            INTEGER),
        impl(nullMissingHandling(DateTimeFunctions::exprWeek), INTEGER, DATE, INTEGER),
        impl(nullMissingHandling(DateTimeFunctions::exprWeek), INTEGER, TIMESTAMP, INTEGER),
        impl(nullMissingHandling(DateTimeFunctions::exprWeek), INTEGER, STRING, INTEGER));
  }

  private DefaultFunctionResolver weekday() {
    return define(
        BuiltinFunctionName.WEEKDAY.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    new ExprIntegerValue(
                        formatNow(functionProperties.getQueryStartClock()).getDayOfWeek().getValue()
                            - 1)),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprWeekday), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprWeekday), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprWeekday), INTEGER, STRING));
  }

  /** YEAR(STRING/DATE/TIMESTAMP). return the year for date (1000-9999). */
  private DefaultFunctionResolver year() {
    return define(
        BuiltinFunctionName.YEAR.getName(),
        impl(nullMissingHandling(DateTimeFunctions::exprYear), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprYear), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprYear), INTEGER, STRING));
  }

  /** YEARWEEK(DATE[,mode]). return the week number for date. */
  private DefaultFunctionResolver yearweek() {
    return define(
        BuiltinFunctionName.YEARWEEK.getName(),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, arg) ->
                    yearweekToday(
                        DEFAULT_WEEK_OF_YEAR_MODE, functionProperties.getQueryStartClock())),
            INTEGER,
            TIME),
        impl(nullMissingHandling(DateTimeFunctions::exprYearweekWithoutMode), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunctions::exprYearweekWithoutMode), INTEGER, TIMESTAMP),
        impl(nullMissingHandling(DateTimeFunctions::exprYearweekWithoutMode), INTEGER, STRING),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, time, modeArg) ->
                    yearweekToday(modeArg, functionProperties.getQueryStartClock())),
            INTEGER,
            TIME,
            INTEGER),
        impl(nullMissingHandling(DateTimeFunctions::exprYearweek), INTEGER, DATE, INTEGER),
        impl(nullMissingHandling(DateTimeFunctions::exprYearweek), INTEGER, TIMESTAMP, INTEGER),
        impl(nullMissingHandling(DateTimeFunctions::exprYearweek), INTEGER, STRING, INTEGER));
  }

  /**
   * Formats date according to format specifier. First argument is date, second is format.<br>
   * Detailed supported signatures:<br>
   * (STRING, STRING) -> STRING<br>
   * (DATE, STRING) -> STRING<br>
   * (TIME, STRING) -> STRING<br>
   * (TIMESTAMP, STRING) -> STRING
   */
  private DefaultFunctionResolver date_format() {
    return define(
        BuiltinFunctionName.DATE_FORMAT.getName(),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedDate), STRING, STRING, STRING),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedDate), STRING, DATE, STRING),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (functionProperties, time, formatString) ->
                    DateTimeFormatterUtil.getFormattedDateOfToday(
                        formatString, time, functionProperties.getQueryStartClock())),
            STRING,
            TIME,
            STRING),
        impl(
            nullMissingHandling(DateTimeFormatterUtil::getFormattedDate),
            STRING,
            TIMESTAMP,
            STRING));
  }

  private ExprValue dayOfMonthToday(Clock clock) {
    return new ExprIntegerValue(LocalDateTime.now(clock).getDayOfMonth());
  }

  private ExprValue dayOfYearToday(Clock clock) {
    return new ExprIntegerValue(LocalDateTime.now(clock).getDayOfYear());
  }

  private ExprValue weekOfYearToday(ExprValue mode, Clock clock) {
    return new ExprIntegerValue(
        CalendarLookup.getWeekNumber(mode.integerValue(), LocalDateTime.now(clock).toLocalDate()));
  }

  /**
   * Day of Week implementation for ExprValue when passing in an arguemt of type TIME.
   *
   * @param clock Current clock taken from function properties
   * @return ExprValue.
   */
  private ExprValue dayOfWeekToday(Clock clock) {
    return new ExprIntegerValue((formatNow(clock).getDayOfWeek().getValue() % 7) + 1);
  }

  /**
   * DATE_ADD function implementation for ExprValue.
   *
   * @param functionProperties An FunctionProperties object.
   * @param datetime ExprValue of Date/Time/Timestamp type.
   * @param interval ExprValue of Interval type, the temporal amount to add.
   * @return Timestamp resulted from `interval` added to `timestamp`.
   */
  private ExprValue exprAddDateInterval(
      FunctionProperties functionProperties, ExprValue datetime, ExprValue interval) {
    return exprDateApplyInterval(functionProperties, datetime, interval.intervalValue(), true);
  }

  /**
   * Adds or subtracts `interval` to/from `timestamp`.
   *
   * @param functionProperties An FunctionProperties object.
   * @param datetime A Date/Time/Timestamp value to change.
   * @param interval An Interval to isAdd or subtract.
   * @param isAdd A flag: true to isAdd, false to subtract.
   * @return Timestamp calculated.
   */
  private ExprValue exprDateApplyInterval(
      FunctionProperties functionProperties,
      ExprValue datetime,
      TemporalAmount interval,
      Boolean isAdd) {
    var dt =
        extractTimestamp(datetime, functionProperties).atZone(ZoneOffset.UTC).toLocalDateTime();
    return new ExprTimestampValue(isAdd ? dt.plus(interval) : dt.minus(interval));
  }

  /**
   * Formats date according to format specifier. First argument is time, second is format.<br>
   * Detailed supported signatures:<br>
   * (STRING, STRING) -> STRING<br>
   * (DATE, STRING) -> STRING<br>
   * (TIME, STRING) -> STRING<br>
   * (TIMESTAMP, STRING) -> STRING
   */
  private DefaultFunctionResolver time_format() {
    return define(
        BuiltinFunctionName.TIME_FORMAT.getName(),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedTime), STRING, STRING, STRING),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedTime), STRING, DATE, STRING),
        impl(nullMissingHandling(DateTimeFormatterUtil::getFormattedTime), STRING, TIME, STRING),
        impl(
            nullMissingHandling(DateTimeFormatterUtil::getFormattedTime),
            STRING,
            TIMESTAMP,
            STRING));
  }

  /**
   * ADDDATE function implementation for ExprValue.
   *
   * @param functionProperties An FunctionProperties object.
   * @param datetime ExprValue of Time/Date/Timestamp type.
   * @param days ExprValue of Long type, representing the number of days to add.
   * @return Date/Timestamp resulted from days added to `timestamp`.
   */
  private ExprValue exprAddDateDays(
      FunctionProperties functionProperties, ExprValue datetime, ExprValue days) {
    return exprDateApplyDays(functionProperties, datetime, days.longValue(), true);
  }

  /**
   * Adds or subtracts `days` to/from `timestamp`.
   *
   * @param functionProperties An FunctionProperties object.
   * @param datetime A Date/Time/Timestamp value to change.
   * @param days A days amount to add or subtract.
   * @param isAdd A flag: true to add, false to subtract.
   * @return Timestamp calculated.
   */
  private ExprValue exprDateApplyDays(
      FunctionProperties functionProperties, ExprValue datetime, Long days, Boolean isAdd) {
    if (datetime.type() == DATE) {
      return new ExprDateValue(
          isAdd ? datetime.dateValue().plusDays(days) : datetime.dateValue().minusDays(days));
    }
    var dt =
        extractTimestamp(datetime, functionProperties).atZone(ZoneOffset.UTC).toLocalDateTime();
    return new ExprTimestampValue(isAdd ? dt.plusDays(days) : dt.minusDays(days));
  }

  /**
   * Adds or subtracts time to/from date and returns the result.
   *
   * @param functionProperties A FunctionProperties object.
   * @param temporal A Date/Time/Timestamp value to change.
   * @param temporalDelta A Date/Time/Timestamp object to add/subtract time from.
   * @param isAdd A flag: true to add, false to subtract.
   * @return A value calculated.
   */
  private ExprValue exprApplyTime(
      FunctionProperties functionProperties,
      ExprValue temporal,
      ExprValue temporalDelta,
      Boolean isAdd) {
    var interval = Duration.between(LocalTime.MIN, temporalDelta.timeValue());
    var result =
        isAdd
            ? extractTimestamp(temporal, functionProperties).plus(interval)
            : extractTimestamp(temporal, functionProperties).minus(interval);
    return temporal.type() == TIME
        ? new ExprTimeValue(result.atZone(ZoneOffset.UTC).toLocalTime())
        : new ExprTimestampValue(result);
  }

  /**
   * Adds time to date and returns the result.
   *
   * @param functionProperties A FunctionProperties object.
   * @param temporal A Date/Time/Timestamp value to change.
   * @param temporalDelta A Date/Time/Timestamp object to add time from.
   * @return A value calculated.
   */
  private ExprValue exprAddTime(
      FunctionProperties functionProperties, ExprValue temporal, ExprValue temporalDelta) {
    return exprApplyTime(functionProperties, temporal, temporalDelta, true);
  }

  /**
   * CONVERT_TZ function implementation for ExprValue. Returns null for time zones outside of +13:00
   * and -12:00.
   *
   * @param startingDateTime ExprValue of Timestamp that is being converted from
   * @param fromTz ExprValue of time zone, representing the time to convert from.
   * @param toTz ExprValue of time zone, representing the time to convert to.
   * @return Timestamp that has been converted to the to_tz timezone.
   */
  public static ExprValue exprConvertTZ(ExprValue startingDateTime, ExprValue fromTz, ExprValue toTz) {
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
          (startingDateTime.timestampValue().atZone(ZoneOffset.UTC).toLocalDateTime())
              .atZone(convertedFromTz);
      return new ExprTimestampValue(
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
   * Calculate the value in days from one date to the other. Only the date parts of the values are
   * used in the calculation.
   *
   * @param first The first value.
   * @param second The second value.
   * @return The diff.
   */
  private ExprValue exprDateDiff(
      FunctionProperties functionProperties, ExprValue first, ExprValue second) {
    // java inverses the value, so we have to swap 1 and 2
    return new ExprLongValue(
        DAYS.between(
            extractDate(second, functionProperties), extractDate(first, functionProperties)));
  }

  /**
   * Timestamp implementation for ExprValue.
   *
   * @param timestamp ExprValue of String type.
   * @param timeZone ExprValue of String type (or null).
   * @return ExprValue of date type.
   */
  public static ExprValue exprDateTime(ExprValue timestamp, ExprValue timeZone) {
    String defaultTimeZone = TimeZone.getDefault().getID();

    try {
      LocalDateTime ldtFormatted =
          LocalDateTime.parse(timestamp.stringValue(), DATE_TIME_FORMATTER_STRICT_WITH_TZ);
      if (timeZone.isNull()) {
        return new ExprTimestampValue(ldtFormatted);
      }

      // Used if timestamp field is invalid format.
    } catch (DateTimeParseException e) {
      return ExprNullValue.of();
    }

    ExprValue convertTZResult;
    ExprTimestampValue tz;
    String toTz;

    try {
      ZonedDateTime zdtWithZoneOffset =
          ZonedDateTime.parse(timestamp.stringValue(), DATE_TIME_FORMATTER_STRICT_WITH_TZ);
      ZoneId fromTZ = zdtWithZoneOffset.getZone();

      tz = new ExprTimestampValue(zdtWithZoneOffset.toLocalDateTime());
      toTz = String.valueOf(fromTZ);
    } catch (DateTimeParseException e) {
      tz = new ExprTimestampValue(timestamp.stringValue());
      toTz = defaultTimeZone;
    }
    convertTZResult = exprConvertTZ(tz, new ExprStringValue(toTz), timeZone);

    return convertTZResult;
  }

  /**
   * DateTime implementation for ExprValue without a timezone to convert to.
   *
   * @param dateTime ExprValue of String type.
   * @return ExprValue of date type.
   */
  public static ExprValue exprDateTimeNoTimezone(ExprValue dateTime) {
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
   * @param date ExprValue of Date/String/Time/Timestamp type.
   * @return ExprValue.
   */
  private ExprValue exprDayOfMonth(ExprValue date) {
    return new ExprIntegerValue(date.dateValue().getDayOfMonth());
  }

  /**
   * Day of Week implementation for ExprValue.
   *
   * @param date ExprValue of Date/String/Timstamp type.
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
   * Obtains a formatted long value for a specified part and timestamp for the 'extract' function.
   *
   * @param part is an ExprValue which comes from a defined list of accepted values.
   * @param timestamp the date to be formatted as an ExprValue.
   * @return is a LONG formatted according to the input arguments.
   */
  public static ExprLongValue formatExtractFunction(ExprValue part, ExprValue timestamp) {
    String partName = part.stringValue().toUpperCase();
    LocalDateTime arg = timestamp.timestampValue().atZone(ZoneOffset.UTC).toLocalDateTime();
    String text =
        arg.format(DateTimeFormatter.ofPattern(extract_formats.get(partName), Locale.ENGLISH));

    return new ExprLongValue(Long.parseLong(text));
  }

  /**
   * Implements extract function. Returns a LONG formatted according to the 'part' argument.
   *
   * @param part Literal that determines the format of the outputted LONG.
   * @param timestamp The Date/Timestamp to be formatted.
   * @return A LONG
   */
  private ExprValue exprExtract(ExprValue part, ExprValue timestamp) {
    return formatExtractFunction(part, timestamp);
  }

  /**
   * Implements extract function. Returns a LONG formatted according to the 'part' argument.
   *
   * @param part Literal that determines the format of the outputted LONG.
   * @param time The time to be formatted.
   * @return A LONG
   */
  private ExprValue exprExtractForTime(
      FunctionProperties functionProperties, ExprValue part, ExprValue time) {
    return formatExtractFunction(
        part, new ExprTimestampValue(extractTimestamp(time, functionProperties)));
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
    return new ExprTimestampValue(exprFromUnixTimeImpl(time));
  }

  private LocalDateTime exprFromUnixTimeImpl(ExprValue time) {
    return LocalDateTime.ofInstant(
            Instant.ofEpochSecond((long) Math.floor(time.doubleValue())), ZoneOffset.UTC)
        .withNano((int) ((time.doubleValue() % 1) * 1E9));
  }

  private ExprValue exprFromUnixTimeFormat(ExprValue time, ExprValue format) {
    var value = exprFromUnixTime(time);
    if (value.equals(ExprNullValue.of())) {
      return ExprNullValue.of();
    }
    return DateTimeFormatterUtil.getFormattedDate(value, format);
  }

  /**
   * get_format implementation for ExprValue.
   *
   * @param type ExprValue of the type.
   * @param format ExprValue of Time/String type
   * @return ExprValue..
   */
  private ExprValue exprGetFormat(ExprValue type, ExprValue format) {
    if (formats.contains(type.stringValue().toLowerCase(), format.stringValue().toLowerCase())) {
      return new ExprStringValue(
          formats.get(type.stringValue().toLowerCase(), format.stringValue().toLowerCase()));
    }

    return ExprNullValue.of();
  }

  /**
   * Hour implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprHour(ExprValue time) {
    return new ExprIntegerValue(HOURS.between(LocalTime.MIN, time.timeValue()));
  }

  /**
   * Helper function to retrieve the last day of a month based on a LocalDate argument.
   *
   * @param today a LocalDate.
   * @return a LocalDate associated with the last day of the month for the given input.
   */
  private LocalDate getLastDay(LocalDate today) {
    return LocalDate.of(
        today.getYear(), today.getMonth(), today.getMonth().length(today.isLeapYear()));
  }

  /**
   * Returns a DATE for the last day of the month of a given argument.
   *
   * @param timestamp A DATE/TIMESTAMP/STRING ExprValue.
   * @return An DATE value corresponding to the last day of the month of the given argument.
   */
  private ExprValue exprLastDay(ExprValue timestamp) {
    return new ExprDateValue(getLastDay(timestamp.dateValue()));
  }

  /**
   * Returns a DATE for the last day of the current month.
   *
   * @param clock The clock for the query start time from functionProperties.
   * @return An DATE value corresponding to the last day of the month of the given argument.
   */
  private ExprValue exprLastDayToday(Clock clock) {
    return new ExprDateValue(getLastDay(formatNow(clock).toLocalDate()));
  }

  /**
   * Following MySQL, function receives arguments of type double and rounds them before use.<br>
   * Furthermore:<br>
   *
   * <ul>
   *   <li>zero year interpreted as 2000
   *   <li>negative year is not accepted
   *   <li>@dayOfYear should be greater than 1
   *   <li>if @dayOfYear is greater than 365/366, calculation goes to the next year(s)
   * </ul>
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
    return new ExprDateValue(LocalDate.ofYearDay((int) year, 1).plusDays(dayOfYear - 1));
  }

  /**
   * Following MySQL, function receives arguments of type double. @hour and @minute are rounded,
   * while @second used as is, including fraction part.
   *
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
    return new ExprTimeValue(
        LocalTime.parse(
            String.format("%02d:%02d:%012.9f", hour, minute, second), DateTimeFormatter.ISO_TIME));
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
    return new ExprIntegerValue((MINUTES.between(LocalTime.MIN, time.timeValue()) % 60));
  }

  /**
   * Minute_of_day implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprMinuteOfDay(ExprValue time) {
    return new ExprIntegerValue(MINUTES.between(LocalTime.MIN, time.timeValue()));
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

  private LocalDate parseDatePeriod(Integer period) {
    var input = period.toString();
    // MySQL undocumented: if year is not specified or has 1 digit - 2000/200x is assumed
    if (input.length() <= 5) {
      input = String.format("200%05d", period);
    }
    try {
      return LocalDate.parse(input, DATE_FORMATTER_SHORT_YEAR);
    } catch (DateTimeParseException ignored) {
      // nothing to do, try another format
    }
    try {
      return LocalDate.parse(input, DATE_FORMATTER_LONG_YEAR);
    } catch (DateTimeParseException ignored) {
      return null;
    }
  }

  /**
   * Adds N months to period P (in the format YYMM or YYYYMM). Returns a value in the format YYYYMM.
   *
   * @param period Period in the format YYMM or YYYYMM.
   * @param months Amount of months to add.
   * @return ExprIntegerValue.
   */
  private ExprValue exprPeriodAdd(ExprValue period, ExprValue months) {
    // We should add a day to make string parsable and remove it afterwards
    var input = period.integerValue() * 100 + 1; // adds 01 to end of the string
    var parsedDate = parseDatePeriod(input);
    if (parsedDate == null) {
      return ExprNullValue.of();
    }
    var res = DATE_FORMATTER_LONG_YEAR.format(parsedDate.plusMonths(months.integerValue()));
    return new ExprIntegerValue(
        Integer.parseInt(
            res.substring(0, res.length() - 2))); // Remove the day part, .eg. 20070101 -> 200701
  }

  /**
   * Returns the number of months between periods P1 and P2. P1 and P2 should be in the format YYMM
   * or YYYYMM.
   *
   * @param period1 Period in the format YYMM or YYYYMM.
   * @param period2 Period in the format YYMM or YYYYMM.
   * @return ExprIntegerValue.
   */
  private ExprValue exprPeriodDiff(ExprValue period1, ExprValue period2) {
    var parsedDate1 = parseDatePeriod(period1.integerValue() * 100 + 1);
    var parsedDate2 = parseDatePeriod(period2.integerValue() * 100 + 1);
    if (parsedDate1 == null || parsedDate2 == null) {
      return ExprNullValue.of();
    }
    return new ExprIntegerValue(MONTHS.between(parsedDate2, parsedDate1));
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
   * Returns TIME value of sec_to_time function for an INTEGER or LONG arguments.
   *
   * @param totalSeconds The total number of seconds
   * @return A TIME value
   */
  private ExprValue exprSecToTime(ExprValue totalSeconds) {
    return new ExprTimeValue(LocalTime.MIN.plus(Duration.ofSeconds(totalSeconds.longValue())));
  }

  /**
   * Helper function which obtains the decimal portion of the seconds value passed in. Uses
   * BigDecimal to prevent issues with math on floating point numbers. Return is formatted to be
   * used with Duration.ofSeconds();
   *
   * @param seconds and ExprDoubleValue or ExprFloatValue for the seconds
   * @return A LONG representing the nanoseconds portion
   */
  private long formatNanos(ExprValue seconds) {
    // Convert ExprValue to BigDecimal
    BigDecimal formattedNanos = BigDecimal.valueOf(seconds.doubleValue());
    // Extract only the nanosecond part
    formattedNanos = formattedNanos.subtract(BigDecimal.valueOf(formattedNanos.intValue()));

    return formattedNanos.scaleByPowerOfTen(9).longValue();
  }

  /**
   * Returns TIME value of sec_to_time function for FLOAT or DOUBLE arguments.
   *
   * @param totalSeconds The total number of seconds
   * @return A TIME value
   */
  private ExprValue exprSecToTimeWithNanos(ExprValue totalSeconds) {
    long nanos = formatNanos(totalSeconds);

    return new ExprTimeValue(
        LocalTime.MIN.plus(Duration.ofSeconds(totalSeconds.longValue(), nanos)));
  }

  /**
   * Second implementation for ExprValue.
   *
   * @param time ExprValue of Time/String type.
   * @return ExprValue.
   */
  private ExprValue exprSecond(ExprValue time) {
    return new ExprIntegerValue((SECONDS.between(LocalTime.MIN, time.timeValue()) % 60));
  }

  /**
   * SUBDATE function implementation for ExprValue.
   *
   * @param functionProperties An FunctionProperties object.
   * @param date ExprValue of Time/Date/Timestamp type.
   * @param days ExprValue of Long type, representing the number of days to subtract.
   * @return Date/Timestamp resulted from days subtracted to date.
   */
  private ExprValue exprSubDateDays(
      FunctionProperties functionProperties, ExprValue date, ExprValue days) {
    return exprDateApplyDays(functionProperties, date, days.longValue(), false);
  }

  /**
   * DATE_SUB function implementation for ExprValue.
   *
   * @param functionProperties An FunctionProperties object.
   * @param datetime ExprValue of Time/Date/Timestamp type.
   * @param expr ExprValue of Interval type, the temporal amount to subtract.
   * @return Timestamp resulted from expr subtracted to `timestamp`.
   */
  private ExprValue exprSubDateInterval(
      FunctionProperties functionProperties, ExprValue datetime, ExprValue expr) {
    return exprDateApplyInterval(functionProperties, datetime, expr.intervalValue(), false);
  }

  /**
   * Subtracts expr2 from expr1 and returns the result.
   *
   * @param temporal A Date/Time/Timestamp value to change.
   * @param temporalDelta A Date/Time/Timestamp to subtract time from.
   * @return A value calculated.
   */
  private ExprValue exprSubTime(
      FunctionProperties functionProperties, ExprValue temporal, ExprValue temporalDelta) {
    return exprApplyTime(functionProperties, temporal, temporalDelta, false);
  }

  private ExprValue exprStrToDate(
      FunctionProperties fp, ExprValue dateTimeExpr, ExprValue formatStringExp) {
    return DateTimeFormatterUtil.parseStringWithDateOrTime(fp, dateTimeExpr, formatStringExp);
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
   * Calculate the time difference between two times.
   *
   * @param first The first value.
   * @param second The second value.
   * @return The diff.
   */
  private ExprValue exprTimeDiff(ExprValue first, ExprValue second) {
    // java inverses the value, so we have to swap 1 and 2
    return new ExprTimeValue(
        LocalTime.MIN.plus(Duration.between(second.timeValue(), first.timeValue())));
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

  private ExprValue exprTimestampAdd(
      ExprValue partExpr, ExprValue amountExpr, ExprValue datetimeExpr) {
    String part = partExpr.stringValue();
    int amount = amountExpr.integerValue();
    LocalDateTime timestamp =
        datetimeExpr.timestampValue().atZone(ZoneOffset.UTC).toLocalDateTime();
    ChronoUnit temporalUnit;

    switch (part) {
      case "MICROSECOND":
        temporalUnit = MICROS;
        break;
      case "SECOND":
        temporalUnit = SECONDS;
        break;
      case "MINUTE":
        temporalUnit = MINUTES;
        break;
      case "HOUR":
        temporalUnit = HOURS;
        break;
      case "DAY":
        temporalUnit = DAYS;
        break;
      case "WEEK":
        temporalUnit = WEEKS;
        break;
      case "MONTH":
        temporalUnit = MONTHS;
        break;
      case "QUARTER":
        temporalUnit = MONTHS;
        amount *= 3;
        break;
      case "YEAR":
        temporalUnit = YEARS;
        break;
      default:
        return ExprNullValue.of();
    }
    return new ExprTimestampValue(timestamp.plus(amount, temporalUnit));
  }

  private ExprValue exprTimestampAddForTimeType(
      Clock clock, ExprValue partExpr, ExprValue amountExpr, ExprValue timeExpr) {
    LocalDateTime datetime = LocalDateTime.of(formatNow(clock).toLocalDate(), timeExpr.timeValue());
    return exprTimestampAdd(partExpr, amountExpr, new ExprTimestampValue(datetime));
  }

  private ExprValue getTimeDifference(String part, LocalDateTime startTime, LocalDateTime endTime) {
    long returnVal;
    switch (part) {
      case "MICROSECOND":
        returnVal = MICROS.between(startTime, endTime);
        break;
      case "SECOND":
        returnVal = SECONDS.between(startTime, endTime);
        break;
      case "MINUTE":
        returnVal = MINUTES.between(startTime, endTime);
        break;
      case "HOUR":
        returnVal = HOURS.between(startTime, endTime);
        break;
      case "DAY":
        returnVal = DAYS.between(startTime, endTime);
        break;
      case "WEEK":
        returnVal = WEEKS.between(startTime, endTime);
        break;
      case "MONTH":
        returnVal = MONTHS.between(startTime, endTime);
        break;
      case "QUARTER":
        returnVal = MONTHS.between(startTime, endTime) / 3;
        break;
      case "YEAR":
        returnVal = YEARS.between(startTime, endTime);
        break;
      default:
        return ExprNullValue.of();
    }
    return new ExprLongValue(returnVal);
  }

  private ExprValue exprTimestampDiff(
      ExprValue partExpr, ExprValue startTimeExpr, ExprValue endTimeExpr) {
    return getTimeDifference(
        partExpr.stringValue(),
        startTimeExpr.timestampValue().atZone(ZoneOffset.UTC).toLocalDateTime(),
        endTimeExpr.timestampValue().atZone(ZoneOffset.UTC).toLocalDateTime());
  }

  private ExprValue exprTimestampDiffForTimeType(
      FunctionProperties fp, ExprValue partExpr, ExprValue startTimeExpr, ExprValue endTimeExpr) {
    return getTimeDifference(
        partExpr.stringValue(),
        extractTimestamp(startTimeExpr, fp).atZone(ZoneOffset.UTC).toLocalDateTime(),
        extractTimestamp(endTimeExpr, fp).atZone(ZoneOffset.UTC).toLocalDateTime());
  }

  /**
   * UTC_DATE implementation for ExprValue.
   *
   * @param functionProperties FunctionProperties.
   * @return ExprValue.
   */
  private ExprValue exprUtcDate(FunctionProperties functionProperties) {
    return new ExprDateValue(exprUtcTimeStamp(functionProperties).dateValue());
  }

  /**
   * UTC_TIME implementation for ExprValue.
   *
   * @param functionProperties FunctionProperties.
   * @return ExprValue.
   */
  private ExprValue exprUtcTime(FunctionProperties functionProperties) {
    return new ExprTimeValue(exprUtcTimeStamp(functionProperties).timeValue());
  }

  /**
   * UTC_TIMESTAMP implementation for ExprValue.
   *
   * @param functionProperties FunctionProperties.
   * @return ExprValue.
   */
  private ExprValue exprUtcTimeStamp(FunctionProperties functionProperties) {
    var zdt =
        ZonedDateTime.now(functionProperties.getQueryStartClock())
            .withZoneSameInstant(ZoneOffset.UTC);
    return new ExprTimestampValue(zdt.toLocalDateTime());
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
   * To_seconds implementation for ExprValue.
   *
   * @param date ExprValue of Date/Timestamp/String type.
   * @return ExprValue.
   */
  private ExprValue exprToSeconds(ExprValue date) {
    return new ExprLongValue(
        date.timestampValue().atOffset(ZoneOffset.UTC).toEpochSecond()
            + DAYS_0000_TO_1970 * SECONDS_PER_DAY);
  }

  /**
   * Helper function to determine the correct formatter for date arguments passed in as integers.
   *
   * @param dateAsInt is an integer formatted as one of YYYYMMDD, YYMMDD, YMMDD, MMDD, MDD
   * @return is a DateTimeFormatter that can parse the input.
   */
  private DateTimeFormatter getFormatter(int dateAsInt) {
    int length = String.format("%d", dateAsInt).length();

    if (length > 8) {
      throw new DateTimeException("Integer argument was out of range");
    }

    // Check below from YYYYMMDD - MMDD which format should be used
    switch (length) {
        // Check if dateAsInt is at least 8 digits long
      case FULL_DATE_LENGTH:
        return DATE_FORMATTER_LONG_YEAR;

        // Check if dateAsInt is at least 6 digits long
      case SHORT_DATE_LENGTH:
        return DATE_FORMATTER_SHORT_YEAR;

        // Check if dateAsInt is at least 5 digits long
      case SINGLE_DIGIT_YEAR_DATE_LENGTH:
        return DATE_FORMATTER_SINGLE_DIGIT_YEAR;

        // Check if dateAsInt is at least 4 digits long
      case NO_YEAR_DATE_LENGTH:
        return DATE_FORMATTER_NO_YEAR;

        // Check if dateAsInt is at least 3 digits long
      case SINGLE_DIGIT_MONTH_DATE_LENGTH:
        return DATE_FORMATTER_SINGLE_DIGIT_MONTH;

      default:
        break;
    }

    throw new DateTimeException("No Matching Format");
  }

  /**
   * To_seconds implementation with an integer argument for ExprValue.
   *
   * @param dateExpr ExprValue of an Integer/Long formatted for a date (e.g., 950501 = 1995-05-01)
   * @return ExprValue.
   */
  private ExprValue exprToSecondsForIntType(ExprValue dateExpr) {
    try {
      // Attempt to parse integer argument as date
      LocalDate date =
          LocalDate.parse(
              String.valueOf(dateExpr.integerValue()), getFormatter(dateExpr.integerValue()));

      return new ExprLongValue(
          date.toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) + DAYS_0000_TO_1970 * SECONDS_PER_DAY);

    } catch (DateTimeException ignored) {
      // Return null if parsing error
      return ExprNullValue.of();
    }
  }

  /**
   * Week for date implementation for ExprValue.
   *
   * @param date ExprValue of Date/Timestamp/String type.
   * @param mode ExprValue of Integer type.
   */
  private ExprValue exprWeek(ExprValue date, ExprValue mode) {
    return new ExprIntegerValue(
        CalendarLookup.getWeekNumber(mode.integerValue(), date.dateValue()));
  }

  /**
   * Weekday implementation for ExprValue.
   *
   * @param date ExprValue of Date/String/Timstamp type.
   * @return ExprValue.
   */
  private ExprValue exprWeekday(ExprValue date) {
    return new ExprIntegerValue(date.dateValue().getDayOfWeek().getValue() - 1);
  }

  private ExprValue unixTimeStamp(Clock clock) {
    return new ExprLongValue(Instant.now(clock).getEpochSecond());
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

  public static Double transferUnixTimeStampFromDoubleInput(Double value) {
    var format = new DecimalFormat("0.#");
    format.setMinimumFractionDigits(0);
    format.setMaximumFractionDigits(6);
    String input = format.format(value);
    double fraction = 0;
    if (input.contains(".")) {
      // Keeping fraction second part and adding it to the result, don't parse it
      // Because `toEpochSecond` returns only `long`
      // input = 12345.6789 becomes input = 12345 and fraction = 0.6789
      fraction = value - Math.round(Math.ceil(value));
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

  private Double unixTimeStampOfImpl(ExprValue value) {
    // Also, according to MySQL documentation:
    //    The date argument may be a DATE, DATETIME, or TIMESTAMP ...
    switch ((ExprCoreType) value.type()) {
      case DATE:
        return value.dateValue().toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) + 0d;
      case TIMESTAMP:
        return value.timestampValue().getEpochSecond() + value.timestampValue().getNano() / 1E9;
      default:
        //     ... or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format.
        //     If the argument includes a time part, it may optionally include a fractional
        //     seconds part.
        return transferUnixTimeStampFromDoubleInput(value.doubleValue());
    }
  }

  /**
   * Week for date implementation for ExprValue. When mode is not specified default value mode 0 is
   * used for default_week_format.
   *
   * @param date ExprValue of Date/Timestamp/String type.
   * @return ExprValue.
   */
  private ExprValue exprWeekWithoutMode(ExprValue date) {
    return exprWeek(date, DEFAULT_WEEK_OF_YEAR_MODE);
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

  /**
   * Helper function to extract the yearweek output from a given date.
   *
   * @param date is a LocalDate input argument.
   * @param mode is an integer containing the mode used to parse the LocalDate.
   * @return is a long containing the formatted output for the yearweek function.
   */
  private ExprIntegerValue extractYearweek(LocalDate date, int mode) {
    // Needed to align with MySQL. Due to how modes for this function work.
    // See description of modes here ...
    // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_week
    int modeJava = CalendarLookup.getWeekNumber(mode, date) != 0 ? mode : mode <= 4 ? 2 : 7;

    int formatted =
        CalendarLookup.getYearNumber(modeJava, date) * 100
            + CalendarLookup.getWeekNumber(modeJava, date);

    return new ExprIntegerValue(formatted);
  }

  /**
   * Yearweek for date implementation for ExprValue.
   *
   * @param date ExprValue of Date/Time/Timestamp/String type.
   * @param mode ExprValue of Integer type.
   */
  private ExprValue exprYearweek(ExprValue date, ExprValue mode) {
    return extractYearweek(date.dateValue(), mode.integerValue());
  }

  /**
   * Yearweek for date implementation for ExprValue. When mode is not specified default value mode 0
   * is used.
   *
   * @param date ExprValue of Date/Time/Timestamp/String type.
   * @return ExprValue.
   */
  private ExprValue exprYearweekWithoutMode(ExprValue date) {
    return exprYearweek(date, new ExprIntegerValue(0));
  }

  private ExprValue yearweekToday(ExprValue mode, Clock clock) {
    return extractYearweek(LocalDateTime.now(clock).toLocalDate(), mode.integerValue());
  }

  private ExprValue monthOfYearToday(Clock clock) {
    return new ExprIntegerValue(LocalDateTime.now(clock).getMonthValue());
  }

  private LocalDateTime formatNow(Clock clock) {
    return formatNow(clock, 0);
  }

  /**
   * Prepare LocalDateTime value. Truncate fractional second part according to the argument.
   *
   * @param fsp argument is given to specify a fractional seconds precision from 0 to 6, the return
   *     value includes a fractional seconds part of that many digits.
   * @return LocalDateTime object.
   */
  private LocalDateTime formatNow(Clock clock, Integer fsp) {
    var res = LocalDateTime.now(clock);
    var defaultPrecision = 9; // There are 10^9 nanoseconds in one second
    if (fsp < 0 || fsp > 6) { // Check that the argument is in the allowed range [0, 6]
      throw new IllegalArgumentException(
          String.format("Invalid `fsp` value: %d, allowed 0 to 6", fsp));
    }
    var nano =
        new BigDecimal(res.getNano())
            .setScale(fsp - defaultPrecision, RoundingMode.DOWN)
            .intValue();
    return res.withNano(nano);
  }
}
