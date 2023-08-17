/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_OFFSET;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.FunctionProperties;

class TimeStampDiffTest extends ExpressionTestBase {

  // Helper function to create an argument based on a passed in interval type
  private static ExprValue generateArg(
      String intervalType, String argType, LocalDateTime base, int added) {
    LocalDateTime arg;
    switch (intervalType) {
      case "MICROSECOND":
        arg = base.plusNanos(added * 1000);
        break;
      case "SECOND":
        arg = base.plusSeconds(added);
        break;
      case "MINUTE":
        arg = base.plusMinutes(added);
        break;
      case "HOUR":
        arg = base.plusHours(added);
        break;
      case "DAY":
        arg = base.plusDays(added);
        break;
      case "WEEK":
        arg = base.plusWeeks(added);
        break;
      case "MONTH":
        arg = base.plusMonths(added);
        break;
      case "QUARTER":
        arg = base.plusMonths(added * 3);
        break;
      case "YEAR":
        arg = base.plusYears(added);
        break;
      default:
        throw new SemanticCheckException(
            String.format("%s is not a valid interval type.", intervalType));
    }

    switch (argType) {
      case "TIME":
        return new ExprTimeValue(arg.toLocalTime());
      case "TIMESTAMP":
        return new ExprTimestampValue(arg.toInstant(UTC_ZONE_OFFSET));
      case "DATE":
        return new ExprDateValue(arg.toLocalDate());
      case "STRING":
        return new ExprStringValue(
            String.format(
                "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                arg.getYear(),
                arg.getMonthValue(),
                arg.getDayOfMonth(),
                arg.getHour(),
                arg.getMinute(),
                arg.getSecond(),
                arg.getNano() / 1000));
      default:
        throw new SemanticCheckException(
            String.format("%s is not a valid ExprCoreValueType.", argType));
    }
  }

  // Generate test data to test all permutations for args (intervalType, arg1, arg2)
  private static Stream<Arguments> getGeneralTestDataForTimestampDiff() {

    // Needs to be initialized with a value to prevent a null pointer exception.
    Stream<Arguments> testData =
        Stream.of(
            Arguments.of(
                "DAY",
                new ExprDateValue("2000-01-01 00:00:00"),
                new ExprDateValue("2000-01-01"),
                0));

    final String[] timeIntervalTypes = {"MICROSECOND", "SECOND", "MINUTE", "HOUR"};

    final String[] dateIntervalTypes = {"DAY", "WEEK", "MONTH", "QUARTER", "YEAR"};

    final String[] intervalTypes = ArrayUtils.addAll(timeIntervalTypes, dateIntervalTypes);

    // TIME type not included here as it is a special case handled by a different test
    final String[] expressionTypes = {"DATE", "TIMESTAMP", "STRING"};

    final LocalDateTime baseDateTime = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    final int intervalDifference = 5;

    // Iterate through each permutation of argument
    for (String intervalType : intervalTypes) {
      for (String firstArgExpressionType : expressionTypes) {
        for (String secondArgExpressionType : expressionTypes) {

          ExprValue firstArg = generateArg(intervalType, firstArgExpressionType, baseDateTime, 0);
          ExprValue secondArg =
              generateArg(intervalType, secondArgExpressionType, baseDateTime, intervalDifference);

          // If second arg is a DATE and you are using a unit of TIME to measure then expected is 0.
          // The second arg is equal to baseDatetime in this case.
          int expected =
              (secondArgExpressionType == "DATE"
                      && Arrays.asList(timeIntervalTypes).contains(intervalType))
                  ? 0
                  : intervalDifference;

          testData =
              Stream.concat(
                  testData,
                  Stream.of(
                      Arguments.of(intervalType, firstArg, secondArg, expected),
                      Arguments.of(intervalType, secondArg, firstArg, -expected)));
        }
      }
    }

    return testData;
  }

  private static Stream<Arguments> getCornerCaseTestDataForTimestampDiff() {
    return Stream.of(

        // Test around Leap Year
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2019-02-28 00:00:00"),
            new ExprTimestampValue("2019-03-01 00:00:00"),
            1),
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2020-02-28 00:00:00"),
            new ExprTimestampValue("2020-03-01 00:00:00"),
            2),

        // Test around year change
        Arguments.of(
            "SECOND",
            new ExprTimestampValue("2019-12-31 23:59:59"),
            new ExprTimestampValue("2020-01-01 00:00:00"),
            1),
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2019-12-31 23:59:59"),
            new ExprTimestampValue("2020-01-01 00:00:00"),
            0),
        Arguments.of(
            "DAY",
            new ExprTimestampValue("2019-12-31 00:00:00"),
            new ExprTimestampValue("2020-01-01 00:00:00"),
            1));
  }

  private static FunctionExpression timestampdiffQuery(
      FunctionProperties functionProperties,
      String unit,
      ExprValue datetimeExpr1,
      ExprValue datetimeExpr2) {
    return DSL.timestampdiff(
        functionProperties,
        DSL.literal(unit),
        DSL.literal(datetimeExpr1),
        DSL.literal(datetimeExpr2));
  }

  @ParameterizedTest
  @MethodSource({"getGeneralTestDataForTimestampDiff", "getCornerCaseTestDataForTimestampDiff"})
  public void testTimestampdiff(
      String unit, ExprValue datetimeExpr1, ExprValue datetimeExpr2, long expected) {
    FunctionExpression expr =
        timestampdiffQuery(functionProperties, unit, datetimeExpr1, datetimeExpr2);
    assertEquals(expected, eval(expr).longValue());
  }

  private static Stream<Arguments> getUnits() {
    return Stream.of(
        Arguments.of("MICROSECOND"),
        Arguments.of("SECOND"),
        Arguments.of("MINUTE"),
        Arguments.of("HOUR"),
        Arguments.of("DAY"),
        Arguments.of("WEEK"),
        Arguments.of("MONTH"),
        Arguments.of("QUARTER"),
        Arguments.of("YEAR"));
  }

  // Test that Time arg uses today's date with all interval/part arguments
  @ParameterizedTest
  @MethodSource("getUnits")
  public void testTimestampDiffWithTimeType(String unit) {
    LocalDateTime base = LocalDateTime.of(LocalDate.now(), LocalTime.of(10, 11, 12));

    ExprValue timeExpr = generateArg(unit, "TIME", base, 0);
    ExprValue timestampExpr = generateArg(unit, "TIMESTAMP", base, 0);
    ExprValue dateExpr = generateArg(unit, "TIMESTAMP", base, 0);
    ExprValue datetimeExpr = generateArg(unit, "TIMESTAMP", base, 0);
    ExprValue stringExpr = generateArg(unit, "TIMESTAMP", base, 0);

    ExprValue[] expressions = {timeExpr, timestampExpr, dateExpr, datetimeExpr, stringExpr};

    for (ExprValue arg1 : expressions) {
      for (ExprValue arg2 : expressions) {
        FunctionExpression funcExpr = timestampdiffQuery(functionProperties, unit, arg1, arg2);

        assertEquals(0L, eval(funcExpr).longValue());
      }
    }
  }

  private static Stream<Arguments> getTimestampDiffInvalidArgs() {
    return Stream.of(
        Arguments.of("SECOND", "2023-13-01 10:11:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-40 10:11:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 25:11:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:70:12", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:70", "2000-01-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-13-01 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-40 10:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-01 25:11:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-01 10:70:12"),
        Arguments.of("SECOND", "2023-01-01 10:11:12", "2000-01-01 10:11:70"));
  }

  @ParameterizedTest
  @MethodSource("getTimestampDiffInvalidArgs")
  public void testTimestampDiffWithInvalidTimeArgs(String unit, String arg1, String arg2) {
    FunctionExpression expr =
        timestampdiffQuery(
            functionProperties, unit, new ExprStringValue(arg1), new ExprStringValue(arg2));
    assertThrows(SemanticCheckException.class, () -> eval(expr));
  }

  @Test
  public void testTimestampDiffWithInvalidPartReturnsNull() {
    FunctionExpression expr =
        timestampdiffQuery(
            functionProperties,
            "INVALID",
            new ExprStringValue("2023-01-01 10:11:12"),
            new ExprStringValue("2000-01-01 10:11:12"));
    assertEquals(ExprNullValue.of(), eval(expr));
  }

  // Test that different input types have the same result
  @Test
  public void testDifferentInputTypesHaveSameResult() {
    String part = "SECOND";
    FunctionExpression dateExpr =
        timestampdiffQuery(
            functionProperties,
            part,
            new ExprDateValue("2000-01-01"),
            new ExprDateValue("2000-01-02"));

    FunctionExpression stringExpr =
        timestampdiffQuery(
            functionProperties,
            part,
            new ExprStringValue("2000-01-01 00:00:00"),
            new ExprStringValue("2000-01-02 00:00:00"));

    FunctionExpression timestampExpr =
        timestampdiffQuery(
            functionProperties,
            part,
            new ExprTimestampValue("2000-01-01 00:00:00"),
            new ExprTimestampValue("2000-01-02 00:00:00"));

    assertAll(
        () -> assertEquals(eval(dateExpr), eval(stringExpr)),
        () -> assertEquals(eval(dateExpr), eval(timestampExpr)));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
