/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.TimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;

public class DateTimeComparisonIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
  }

  private final TimeZone testTz = TimeZone.getDefault();
  private final TimeZone systemTz = TimeZone.getTimeZone(System.getProperty("user.timezone"));

  @Before
  public void setTimeZone() {
    TimeZone.setDefault(systemTz);
  }

  @After
  public void resetTimeZone() {
    TimeZone.setDefault(testTz);
  }

  private final String functionCall;
  private final String name;
  private final Boolean expectedResult;

  public DateTimeComparisonIT(
      @Name("functionCall") String functionCall,
      @Name("name") String name,
      @Name("expectedResult") Boolean expectedResult) {
    this.functionCall = functionCall;
    this.name = name;
    this.expectedResult = expectedResult;
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareTwoDates() {
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') = DATE('2020-09-16')", "eq1", true),
            $("DATE('2020-09-16') = DATE('1961-04-12')", "eq2", false),
            $("DATE('2020-09-16') != DATE('1984-12-15')", "neq1", true),
            $("DATE('1961-04-12') != DATE('1984-12-15')", "neq2", true),
            $("DATE('1961-04-12') != DATE('1961-04-12')", "neq3", false),
            $("DATE('1984-12-15') > DATE('1961-04-12')", "gt1", true),
            $("DATE('1984-12-15') > DATE('2020-09-16')", "gt2", false),
            $("DATE('1961-04-12') < DATE('1984-12-15')", "lt1", true),
            $("DATE('1984-12-15') < DATE('1961-04-12')", "lt2", false),
            $("DATE('1984-12-15') >= DATE('1961-04-12')", "gte1", true),
            $("DATE('1984-12-15') >= DATE('1984-12-15')", "gte2", true),
            $("DATE('1984-12-15') >= DATE('2020-09-16')", "gte3", false),
            $("DATE('1961-04-12') <= DATE('1984-12-15')", "lte1", true),
            $("DATE('1961-04-12') <= DATE('1961-04-12')", "lte2", true),
            $("DATE('2020-09-16') <= DATE('1961-04-12')", "lte3", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareTwoTimes() {
    return Arrays.asList(
        $$(
            $("TIME('09:16:37') = TIME('09:16:37')", "eq1", true),
            $("TIME('09:16:37') = TIME('04:12:42')", "eq2", false),
            $("TIME('09:16:37') != TIME('12:15:22')", "neq1", true),
            $("TIME('04:12:42') != TIME('12:15:22')", "neq2", true),
            $("TIME('04:12:42') != TIME('04:12:42')", "neq3", false),
            $("TIME('12:15:22') > TIME('04:12:42')", "gt1", true),
            $("TIME('12:15:22') > TIME('19:16:03')", "gt2", false),
            $("TIME('04:12:42') < TIME('12:15:22')", "lt1", true),
            $("TIME('14:12:38') < TIME('12:15:22')", "lt2", false),
            $("TIME('12:15:22') >= TIME('04:12:42')", "gte1", true),
            $("TIME('12:15:22') >= TIME('12:15:22')", "gte2", true),
            $("TIME('12:15:22') >= TIME('19:16:03')", "gte3", false),
            $("TIME('04:12:42') <= TIME('12:15:22')", "lte1", true),
            $("TIME('04:12:42') <= TIME('04:12:42')", "lte2", true),
            $("TIME('19:16:03') <= TIME('04:12:42')", "lte3", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareTwoTimestamps() {
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('2020-09-16 10:20:30')", "eq1", true),
            $("TIMESTAMP('2020-09-16 10:20:30') = TIMESTAMP('1961-04-12 09:07:00')", "eq2", false),
            $("TIMESTAMP('2020-09-16 10:20:30') != TIMESTAMP('1984-12-15 22:15:07')", "neq1", true),
            $("TIMESTAMP('1984-12-15 22:15:08') != TIMESTAMP('1984-12-15 22:15:07')", "neq2", true),
            $(
                "TIMESTAMP('1961-04-12 09:07:00') != TIMESTAMP('1961-04-12 09:07:00')",
                "neq3",
                false),
            $("TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1961-04-12 22:15:07')", "gt1", true),
            $("TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('1984-12-15 22:15:06')", "gt2", true),
            $("TIMESTAMP('1984-12-15 22:15:07') > TIMESTAMP('2020-09-16 10:20:30')", "gt3", false),
            $("TIMESTAMP('1961-04-12 09:07:00') < TIMESTAMP('1984-12-15 09:07:00')", "lt1", true),
            $("TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1984-12-15 22:15:08')", "lt2", true),
            $("TIMESTAMP('1984-12-15 22:15:07') < TIMESTAMP('1961-04-12 09:07:00')", "lt3", false),
            $("TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1961-04-12 09:07:00')", "gte1", true),
            $("TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('1984-12-15 22:15:07')", "gte2", true),
            $(
                "TIMESTAMP('1984-12-15 22:15:07') >= TIMESTAMP('2020-09-16 10:20:30')",
                "gte3",
                false),
            $("TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1984-12-15 22:15:07')", "lte1", true),
            $("TIMESTAMP('1961-04-12 09:07:00') <= TIMESTAMP('1961-04-12 09:07:00')", "lte2", true),
            $(
                "TIMESTAMP('2020-09-16 10:20:30') <= TIMESTAMP('1961-04-12 09:07:00')",
                "lte3",
                false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareEqTimestampWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16')", "ts_d_t", true),
            $("DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 10:20:30') = DATE('1961-04-12')", "ts_d_f", false),
            $("DATE('1961-04-12') = TIMESTAMP('1984-12-15 22:15:07')", "d_ts_f", false),
            $("TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30')", "ts_t_t", true),
            $("TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", true),
            $("TIMESTAMP('2020-09-16 10:20:30') = TIME('09:07:00')", "ts_t_f", false),
            $("TIME('09:07:00') = TIMESTAMP('1984-12-15 22:15:07')", "t_ts_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareEqDateWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16')", "ts_d_t", true),
            $("DATE('2020-09-16') = TIMESTAMP('1961-04-12 09:07:00')", "d_ts_f", false),
            $("TIMESTAMP('1984-12-15 09:07:00') = DATE('1984-12-15')", "ts_d_f", false),
            $("DATE('" + today + "') = TIME('00:00:00')", "d_t_t", true),
            $("TIME('00:00:00') = DATE('" + today + "')", "t_d_t", true),
            $("DATE('2020-09-16') = TIME('09:07:00')", "d_t_f", false),
            $("TIME('09:07:00') = DATE('" + today + "')", "t_d_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareEqTimeWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIME('10:20:30') = TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", true),
            $("TIMESTAMP('" + today + " 10:20:30') = TIME('10:20:30')", "ts_t_t", true),
            $("TIME('22:15:07') = TIMESTAMP('1984-12-15 22:15:07')", "t_ts_f", false),
            $("TIMESTAMP('1984-12-15 10:20:30') = TIME('10:20:30')", "ts_t_f", false),
            $("TIME('00:00:00') = DATE('" + today + "')", "t_d_t", true),
            $("DATE('" + today + "') = TIME('00:00:00')", "d_t_t", true),
            $("TIME('09:07:00') = DATE('" + today + "')", "t_d_f", false),
            $("DATE('2020-09-16') = TIME('09:07:00')", "d_t_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareNeqTimestampWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 10:20:30') != DATE('1961-04-12')", "ts_d_t", true),
            $("DATE('1961-04-12') != TIMESTAMP('1984-12-15 22:15:07')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16')", "ts_d_f", false),
            $("DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('2020-09-16 10:20:30') != TIME('09:07:00')", "ts_t_t", true),
            $("TIME('09:07:00') != TIMESTAMP('1984-12-15 22:15:07')", "t_ts_t", true),
            $("TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30')", "ts_t_f", false),
            $("TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareNeqDateWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') != TIMESTAMP('1961-04-12 09:07:00')", "d_ts_t", true),
            $("TIMESTAMP('1984-12-15 09:07:00') != DATE('1984-12-15')", "ts_d_t", true),
            $("DATE('2020-09-16') != TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('2020-09-16 00:00:00') != DATE('2020-09-16')", "ts_d_f", false),
            $("DATE('2020-09-16') != TIME('09:07:00')", "d_t_t", true),
            $("TIME('09:07:00') != DATE('" + today + "')", "t_d_t", true),
            $("DATE('" + today + "') != TIME('00:00:00')", "d_t_f", false),
            $("TIME('00:00:00') != DATE('" + today + "')", "t_d_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareNeqTimeWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIME('22:15:07') != TIMESTAMP('1984-12-15 22:15:07')", "t_ts_t", true),
            $("TIMESTAMP('1984-12-15 10:20:30') != TIME('10:20:30')", "ts_t_t", true),
            $("TIME('10:20:30') != TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", false),
            $("TIMESTAMP('" + today + " 10:20:30') != TIME('10:20:30')", "ts_t_f", false),
            $("TIME('09:07:00') != DATE('" + today + "')", "t_d_t", true),
            $("DATE('2020-09-16') != TIME('09:07:00')", "d_t_t", true),
            $("TIME('00:00:00') != DATE('" + today + "')", "t_d_f", false),
            $("DATE('" + today + "') != TIME('00:00:00')", "d_t_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareLtTimestampWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 10:20:30') < DATE('2077-04-12')", "ts_d_t", true),
            $("DATE('1961-04-12') < TIMESTAMP('1984-12-15 22:15:07')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 10:20:30') < DATE('1961-04-12')", "ts_d_f", false),
            $("DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('2020-09-16 10:20:30') < TIME('09:07:00')", "ts_t_t", true),
            $("TIME('09:07:00') < TIMESTAMP('3077-12-15 22:15:07')", "t_ts_t", true),
            $("TIMESTAMP('" + today + " 10:20:30') < TIME('10:20:30')", "ts_t_f", false),
            $("TIME('20:50:40') < TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareLtDateWithOtherTypes() {
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') < TIMESTAMP('3077-04-12 09:07:00')", "d_ts_t", true),
            $("TIMESTAMP('1961-04-12 09:07:00') < DATE('1984-12-15')", "ts_d_t", true),
            $("DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('2077-04-12 09:07:00') < DATE('2020-09-16')", "ts_d_f", false),
            $("DATE('2020-09-16') < TIME('09:07:00')", "d_t_t", true),
            $("TIME('09:07:00') < DATE('3077-04-12')", "t_d_t", true),
            $("DATE('3077-04-12') < TIME('00:00:00')", "d_t_f", false),
            $("TIME('00:00:00') < DATE('2020-09-16')", "t_d_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareLtTimeWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIME('22:15:07') < TIMESTAMP('3077-12-15 22:15:07')", "t_ts_t", true),
            $("TIMESTAMP('1984-12-15 10:20:30') < TIME('10:20:30')", "ts_t_t", true),
            $("TIME('10:20:30') < TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", false),
            $("TIMESTAMP('" + today + " 20:50:42') < TIME('10:20:30')", "ts_t_f", false),
            $("TIME('09:07:00') < DATE('3077-04-12')", "t_d_t", true),
            $("DATE('2020-09-16') < TIME('09:07:00')", "d_t_t", true),
            $("TIME('00:00:00') < DATE('1961-04-12')", "t_d_f", false),
            $("DATE('3077-04-12') < TIME('10:20:30')", "d_t_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareGtTimestampWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 10:20:30') > DATE('1961-04-12')", "ts_d_t", true),
            $("DATE('2020-09-16') > TIMESTAMP('2020-09-15 22:15:07')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 10:20:30') > DATE('2077-04-12')", "ts_d_f", false),
            $("DATE('1961-04-12') > TIMESTAMP('1961-04-12 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('3077-07-08 20:20:30') > TIME('10:20:30')", "ts_t_t", true),
            $("TIME('20:50:40') > TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", true),
            $("TIMESTAMP('" + today + " 10:20:30') > TIME('10:20:30')", "ts_t_f", false),
            $("TIME('09:07:00') > TIMESTAMP('3077-12-15 22:15:07')", "t_ts_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareGtDateWithOtherTypes() {
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') > TIMESTAMP('1961-04-12 09:07:00')", "d_ts_t", true),
            $("TIMESTAMP('2077-04-12 09:07:00') > DATE('2020-09-16')", "ts_d_t", true),
            $("DATE('2020-09-16') > TIMESTAMP('2020-09-16 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('1961-04-12 09:07:00') > DATE('1984-12-15')", "ts_d_f", false),
            $("DATE('3077-04-12') > TIME('00:00:00')", "d_t_t", true),
            $("TIME('00:00:00') > DATE('2020-09-16')", "t_d_t", true),
            $("DATE('2020-09-16') > TIME('09:07:00')", "d_t_f", false),
            $("TIME('09:07:00') > DATE('3077-04-12')", "t_d_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareGtTimeWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIME('22:15:07') > TIMESTAMP('1984-12-15 22:15:07')", "t_ts_t", true),
            $("TIMESTAMP('" + today + " 20:50:42') > TIME('10:20:30')", "ts_t_t", true),
            $("TIME('10:20:30') > TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", false),
            $("TIMESTAMP('1984-12-15 10:20:30') > TIME('10:20:30')", "ts_t_f", false),
            $("TIME('00:00:00') > DATE('1961-04-12')", "t_d_t", true),
            $("DATE('3077-04-12') > TIME('10:20:30')", "d_t_t", true),
            $("TIME('09:07:00') > DATE('3077-04-12')", "t_d_f", false),
            $("DATE('2020-09-16') > TIME('09:07:00')", "d_t_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareLteTimestampWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 10:20:30') <= DATE('2077-04-12')", "ts_d_t", true),
            $("DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 10:20:30') <= DATE('1961-04-12')", "ts_d_f", false),
            $("DATE('2077-04-12') <= TIMESTAMP('1984-12-15 22:15:07')", "d_ts_f", false),
            $("TIMESTAMP('" + today + " 10:20:30') <= TIME('10:20:30')", "ts_t_t", true),
            $("TIME('09:07:00') <= TIMESTAMP('3077-12-15 22:15:07')", "t_ts_t", true),
            $("TIMESTAMP('3077-09-16 10:20:30') <= TIME('09:07:00')", "ts_t_f", false),
            $("TIME('20:50:40') <= TIMESTAMP('" + today + " 10:20:30')", "t_ts_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareLteDateWithOtherTypes() {
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') <= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", true),
            $("TIMESTAMP('1961-04-12 09:07:00') <= DATE('1984-12-15')", "ts_d_t", true),
            $("DATE('2020-09-16') <= TIMESTAMP('1961-04-12 09:07:00')", "d_ts_f", false),
            $("TIMESTAMP('2077-04-12 09:07:00') <= DATE('2020-09-16')", "ts_d_f", false),
            $("DATE('2020-09-16') <= TIME('09:07:00')", "d_t_t", true),
            $("TIME('09:07:00') <= DATE('3077-04-12')", "t_d_t", true),
            $("DATE('3077-04-12') <= TIME('00:00:00')", "d_t_f", false),
            $("TIME('00:00:00') <= DATE('2020-09-16')", "t_d_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareLteTimeWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIME('10:20:30') <= TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", true),
            $("TIMESTAMP('1984-12-15 10:20:30') <= TIME('10:20:30')", "ts_t_t", true),
            $("TIME('22:15:07') <= TIMESTAMP('1984-12-15 22:15:07')", "t_ts_f", false),
            $("TIMESTAMP('" + today + " 20:50:42') <= TIME('10:20:30')", "ts_t_f", false),
            $("TIME('09:07:00') <= DATE('3077-04-12')", "t_d_t", true),
            $("DATE('2020-09-16') <= TIME('09:07:00')", "d_t_t", true),
            $("TIME('00:00:00') <= DATE('1961-04-12')", "t_d_f", false),
            $("DATE('3077-04-12') <= TIME('10:20:30')", "d_t_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareGteTimestampWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIMESTAMP('2020-09-16 10:20:30') >= DATE('1961-04-12')", "ts_d_t", true),
            $("DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", true),
            $("TIMESTAMP('2020-09-16 10:20:30') >= DATE('2077-04-12')", "ts_d_f", false),
            $("DATE('1961-04-11') >= TIMESTAMP('1961-04-12 00:00:00')", "d_ts_f", false),
            $("TIMESTAMP('" + today + " 10:20:30') >= TIME('10:20:30')", "ts_t_t", true),
            $("TIME('20:50:40') >= TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", true),
            $("TIMESTAMP('1977-07-08 10:20:30') >= TIME('10:20:30')", "ts_t_f", false),
            $("TIME('09:07:00') >= TIMESTAMP('3077-12-15 22:15:07')", "t_ts_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareGteDateWithOtherTypes() {
    return Arrays.asList(
        $$(
            $("DATE('2020-09-16') >= TIMESTAMP('2020-09-16 00:00:00')", "d_ts_t", true),
            $("TIMESTAMP('2077-04-12 09:07:00') >= DATE('2020-09-16')", "ts_d_t", true),
            $("DATE('1961-04-12') >= TIMESTAMP('1961-04-12 09:07:00')", "d_ts_f", false),
            $("TIMESTAMP('1961-04-12 09:07:00') >= DATE('1984-12-15')", "ts_d_f", false),
            $("DATE('3077-04-12') >= TIME('00:00:00')", "d_t_t", true),
            $("TIME('00:00:00') >= DATE('2020-09-16')", "t_d_t", true),
            $("DATE('2020-09-16') >= TIME('09:07:00')", "d_t_f", false),
            $("TIME('09:07:00') >= DATE('3077-04-12')", "t_d_f", false)));
  }

  @ParametersFactory(argumentFormatting = "%1$s => %3$s")
  public static Iterable<Object[]> compareGteTimeWithOtherTypes() {
    var today = LocalDate.now().toString();
    return Arrays.asList(
        $$(
            $("TIME('10:20:30') >= TIMESTAMP('" + today + " 10:20:30')", "t_ts_t", true),
            $("TIMESTAMP('" + today + " 20:50:42') >= TIME('10:20:30')", "ts_t_t", true),
            $("TIME('22:15:07') >= TIMESTAMP('3077-12-15 22:15:07')", "t_ts_f", false),
            $("TIMESTAMP('1984-12-15 10:20:30') >= TIME('10:20:30')", "ts_t_f", false),
            $("TIME('00:00:00') >= DATE('1961-04-12')", "t_d_t", true),
            $("DATE('3077-04-12') >= TIME('10:20:30')", "d_t_t", true),
            $("TIME('09:07:00') >= DATE('3077-04-12')", "t_d_f", false),
            $("DATE('2020-09-16') >= TIME('09:07:00')", "d_t_f", false)));
  }

  @Test
  public void testCompare() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval `%s` = %s | fields `%s`",
                TEST_INDEX_DATATYPE_NONNUMERIC, name, functionCall, name));
    verifySchema(result, schema(name, null, "boolean"));
    verifyDataRows(result, rows(expectedResult));
  }
}
