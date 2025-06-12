/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.IsoFields;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.FunctionProperties;

class ExtractTest extends ExpressionTestBase {

  private final String datetimeInput = "2023-02-11 10:11:12.123";

  private final String timeInput = "10:11:12.123";

  private final String dateInput = "2023-02-11";

  private static Stream<Arguments> getDatetimeResultsForExtractFunction() {
    return Stream.of(
        Arguments.of("DAY_MICROSECOND", 11101112123000L),
        Arguments.of("DAY_SECOND", 11101112),
        Arguments.of("DAY_MINUTE", 111011),
        Arguments.of("DAY_HOUR", 1110));
  }

  private static Stream<Arguments> getTimeResultsForExtractFunction() {
    return Stream.of(
        Arguments.of("MICROSECOND", 123000),
        Arguments.of("SECOND", 12),
        Arguments.of("MINUTE", 11),
        Arguments.of("HOUR", 10),
        Arguments.of("SECOND_MICROSECOND", 12123000),
        Arguments.of("MINUTE_MICROSECOND", 1112123000),
        Arguments.of("MINUTE_SECOND", 1112),
        Arguments.of("HOUR_MICROSECOND", 101112123000L),
        Arguments.of("HOUR_SECOND", 101112),
        Arguments.of("HOUR_MINUTE", 1011));
  }

  private static Stream<Arguments> getDateResultsForExtractFunction() {
    return Stream.of(
        Arguments.of("DAY", 11),
        Arguments.of("WEEK", 6),
        Arguments.of("MONTH", 2),
        Arguments.of("QUARTER", 1),
        Arguments.of("YEAR", 2023),
        Arguments.of("YEAR_MONTH", 202302));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource({
    "getDatetimeResultsForExtractFunction",
    "getTimeResultsForExtractFunction",
    "getDateResultsForExtractFunction"
  })
  public void testExtractWithDatetime(String part, long expected) {
    FunctionExpression datetimeExpression =
        DSL.extract(DSL.literal(part), DSL.literal(new ExprDatetimeValue(datetimeInput)));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", DATETIME '2023-02-11 10:11:12.123')", part),
        datetimeExpression.toString());
  }

  private void datePartWithTimeArgQuery(String part, String time, long expected) {
    datePartWithTimeArgQuery(functionProperties, part, time, expected);
  }

  private void datePartWithTimeArgQuery(
      FunctionProperties properties, String part, String time, long expected) {
    ExprTimeValue timeValue = new ExprTimeValue(time);
    FunctionExpression datetimeExpression =
        DSL.extract(properties, DSL.literal(part), DSL.literal(timeValue));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
  }

  @Test
  public void testExtractDatePartWithTimeType() {
    LocalDate now = LocalDate.now(functionProperties.getQueryStartClock());

    datePartWithTimeArgQuery("DAY", timeInput, now.getDayOfMonth());

    datePartWithTimeArgQuery("MONTH", timeInput, now.getMonthValue());

    datePartWithTimeArgQuery("YEAR", timeInput, now.getYear());
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(
      strings = {
        "2009-12-26",
        "2009-12-27",
        "2008-12-28", // Week 52 of week-based-year 2008
        "2009-12-29",
        "2008-12-29", // Week 1 of week-based-year 2009
        "2008-12-31", // Week 1 of week-based-year 2009
        "2009-01-01", // Week 1 of week-based-year 2009
        "2009-01-04", // Week 1 of week-based-year 2009
        "2009-01-05", // Week 2 of week-based-year 2009
        "2025-12-27", //  year with 52 weeks
        "2026-01-01", // year starts on a THURSDAY
        "2028-12-30", //  year with 53 weeks
        "2028-12-31", // year starts in December
        "2029-01-01",
        "2033-12-31", // year with 53 weeks
        "2034-01-01", // January 1st on a SUNDAY
        "2034-12-30", // year with 52 weeks
        "2034-12-31"
      })
  public void testExtractWeekPartWithTimeType(String arg) {

    // setup default date/time properties for the extract function
    ZoneId currentZoneId = ZoneId.systemDefault();
    Instant nowInstant =
        LocalDate.parse(arg).atTime(LocalTime.parse(timeInput)).atZone(currentZoneId).toInstant();
    FunctionProperties properties = new FunctionProperties(nowInstant, currentZoneId);

    // Expected WEEK value should be formated from week-of-week-based-year
    LocalDateTime localDateTime = LocalDateTime.ofInstant(nowInstant, currentZoneId);
    int expected = localDateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);

    // verify
    datePartWithTimeArgQuery(properties, "WEEK", timeInput, expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getDateResultsForExtractFunction")
  public void testExtractWithDate(String part, long expected) {
    FunctionExpression datetimeExpression =
        DSL.extract(DSL.literal(part), DSL.literal(new ExprDateValue(dateInput)));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", DATE '2023-02-11')", part), datetimeExpression.toString());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTimeResultsForExtractFunction")
  public void testExtractWithTime(String part, long expected) {
    FunctionExpression datetimeExpression =
        DSL.extract(
            functionProperties, DSL.literal(part), DSL.literal(new ExprTimeValue(timeInput)));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", TIME '10:11:12.123')", part), datetimeExpression.toString());
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
