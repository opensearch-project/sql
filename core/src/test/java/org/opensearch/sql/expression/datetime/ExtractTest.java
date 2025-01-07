/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

import java.time.LocalDate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

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
        DSL.extract(DSL.literal(part), DSL.literal(new ExprTimestampValue(datetimeInput)));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
    assertEquals(
        String.format("extract(\"%s\", TIMESTAMP '2023-02-11 10:11:12.123')", part),
        datetimeExpression.toString());
  }

  private void datePartWithTimeArgQuery(String part, String time, long expected) {
    ExprTimeValue timeValue = new ExprTimeValue(time);
    FunctionExpression datetimeExpression =
        DSL.extract(functionProperties, DSL.literal(part), DSL.literal(timeValue));

    assertEquals(LONG, datetimeExpression.type());
    assertEquals(expected, eval(datetimeExpression).longValue());
  }

  @Test
  @Disabled("Test is disabled because of issue https://github.com/opensearch-project/sql/issues/2477")
  public void testExtractDatePartWithTimeType() {
    LocalDate now = LocalDate.now(functionProperties.getQueryStartClock());

    datePartWithTimeArgQuery("DAY", timeInput, now.getDayOfMonth());

    // To avoid flaky test, skip the testing in December and January because the WEEK is ISO 8601
    // week-of-week-based-year which is considered to start on a Monday and week 1 is the first week
    // with >3 days. it is possible for early-January dates to be part of the 52nd or 53rd week of
    // the previous year, and for late-December dates to be part of the first week of the next year.
    // For example, 2005-01-02 is part of the 53rd week of year 2004, while 2012-12-31 is part of
    // the first week of 2013
    if (now.getMonthValue() != 1 && now.getMonthValue() != 12) {
      datePartWithTimeArgQuery("WEEK", datetimeInput, now.get(ALIGNED_WEEK_OF_YEAR));
    }

    datePartWithTimeArgQuery("MONTH", timeInput, now.getMonthValue());

    datePartWithTimeArgQuery("YEAR", timeInput, now.getYear());
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
