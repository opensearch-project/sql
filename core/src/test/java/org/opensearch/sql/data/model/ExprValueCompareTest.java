/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.utils.DateTimeUtils.extractTimestamp;

import java.time.LocalDate;
import java.time.Period;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.ExpressionTestBase;

public class ExprValueCompareTest extends ExpressionTestBase {

  @Test
  public void timeValueCompare() {
    assertEquals(0, new ExprTimeValue("18:00:00").compareTo(new ExprTimeValue("18:00:00")));
    assertEquals(1, new ExprTimeValue("19:00:00").compareTo(new ExprTimeValue("18:00:00")));
    assertEquals(-1, new ExprTimeValue("18:00:00").compareTo(new ExprTimeValue("19:00:00")));
  }

  @Test
  public void dateValueCompare() {
    assertEquals(0, new ExprDateValue("2012-08-07").compareTo(new ExprDateValue("2012-08-07")));
    assertEquals(1, new ExprDateValue("2012-08-08").compareTo(new ExprDateValue("2012-08-07")));
    assertEquals(-1, new ExprDateValue("2012-08-07").compareTo(new ExprDateValue("2012-08-08")));
  }

  @Test
  public void timestampValueCompare() {
    assertEquals(
        0,
        new ExprTimestampValue("2012-08-07 18:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 18:00:00")));
    assertEquals(
        1,
        new ExprTimestampValue("2012-08-07 19:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 18:00:00")));
    assertEquals(
        -1,
        new ExprTimestampValue("2012-08-07 18:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 19:00:00")));
  }

  private static Stream<Arguments> getEqualDatetimeValuesOfDifferentTypes() {
    return Stream.of(
        Arguments.of(
            new ExprTimestampValue("1984-11-22 00:00:00"), new ExprDateValue("1984-11-22")),
        Arguments.of(
            new ExprTimestampValue(LocalDate.now() + " 00:00:00"),
            new ExprDateValue(LocalDate.now())),
        Arguments.of(new ExprDateValue(LocalDate.now()), new ExprTimeValue("00:00:00")),
        Arguments.of(
            new ExprTimestampValue("1984-11-22 00:00:00"), new ExprDateValue("1984-11-22")),
        Arguments.of(
            new ExprTimeValue("17:42:15"), new ExprTimestampValue(LocalDate.now() + " 17:42:15")));
  }

  /**
   * We can't compare directly ExprValues of different datetime types, we need to use
   * `FunctionProperties` object to extract comparable values.
   */
  @ParameterizedTest
  @MethodSource("getEqualDatetimeValuesOfDifferentTypes")
  public void compareEqDifferentDateTimeValueTypes(ExprValue left, ExprValue right) {
    assertEquals(
        0,
        extractTimestamp(left, functionProperties)
            .compareTo(extractTimestamp(right, functionProperties)));
    assertEquals(
        0,
        extractTimestamp(right, functionProperties)
            .compareTo(extractTimestamp(left, functionProperties)));
  }

  private static Stream<Arguments> getNotEqualDatetimeValuesOfDifferentTypes() {
    return Stream.of(
        Arguments.of(
            new ExprDateValue("1984-11-22"), new ExprTimestampValue("2020-09-16 17:30:00")),
        Arguments.of(new ExprDateValue("1984-11-22"), new ExprTimeValue("19:14:38")),
        Arguments.of(new ExprTimeValue("19:14:38"), new ExprDateValue(LocalDate.now())),
        Arguments.of(new ExprTimeValue("19:14:38"), new ExprTimestampValue("1984-02-03 04:05:07")),
        Arguments.of(new ExprTimestampValue("2012-08-07 19:14:38"), new ExprTimeValue("09:07:00")),
        Arguments.of(
            new ExprTimestampValue(LocalDate.now() + " 19:14:38"), new ExprTimeValue("09:07:00")),
        Arguments.of(
            new ExprTimestampValue("2012-08-07 00:00:00"), new ExprDateValue("1961-04-12")),
        Arguments.of(
            new ExprTimestampValue("1961-04-12 19:14:38"), new ExprDateValue("1961-04-12")));
  }

  /**
   * We can't compare directly ExprValues of different datetime types, we need to use
   * `FunctionProperties` object to extract comparable values.
   */
  @ParameterizedTest
  @MethodSource("getNotEqualDatetimeValuesOfDifferentTypes")
  public void compareNeqDifferentDateTimeValueTypes(ExprValue left, ExprValue right) {
    assertNotEquals(
        0,
        extractTimestamp(left, functionProperties)
            .compareTo(extractTimestamp(right, functionProperties)));
    assertNotEquals(
        0,
        extractTimestamp(right, functionProperties)
            .compareTo(extractTimestamp(left, functionProperties)));
  }

  @Test
  public void compareDateTimeWithNotADateTime() {
    var exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> new ExprDoubleValue(3.1415).compareTo(new ExprIntervalValue(Period.ofDays(1))));
    assertEquals(
        "compare expected value have same type, but with [DOUBLE, INTERVAL]",
        exception.getMessage());

    exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> new ExprDateValue("1961-04-12").compareTo(new ExprIntegerValue(1)));
    assertEquals(
        "compare expected value have same type, but with [DATE, INTEGER]", exception.getMessage());

    exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> new ExprStringValue("something").compareTo(new ExprTimeValue("10:20:30")));
    assertEquals(
        "compare expected value have same type, but with [STRING, TIME]", exception.getMessage());
  }

  @Test
  public void intValueCompare() {
    assertEquals(0, new ExprIntegerValue(1).compareTo(new ExprIntegerValue(1)));
    assertEquals(1, new ExprIntegerValue(2).compareTo(new ExprIntegerValue(1)));
    assertEquals(-1, new ExprIntegerValue(1).compareTo(new ExprIntegerValue(2)));
  }

  @Test
  public void doubleValueCompare() {
    assertEquals(0, new ExprDoubleValue(1).compareTo(new ExprDoubleValue(1)));
    assertEquals(1, new ExprDoubleValue(2).compareTo(new ExprDoubleValue(1)));
    assertEquals(-1, new ExprDoubleValue(1).compareTo(new ExprDoubleValue(2)));
  }

  private static Stream<Arguments> getEqualNumericValuesOfDifferentTypes() {
    return Stream.of(
        Arguments.of(new ExprIntegerValue(42), new ExprByteValue(42)),
        Arguments.of(new ExprIntegerValue(42), new ExprShortValue(42)),
        Arguments.of(new ExprIntegerValue(42), new ExprLongValue(42)),
        Arguments.of(new ExprIntegerValue(42), new ExprFloatValue(42)),
        Arguments.of(new ExprIntegerValue(42), new ExprDoubleValue(42)));
  }

  @ParameterizedTest
  @MethodSource("getEqualNumericValuesOfDifferentTypes")
  public void compareEqDifferentNumericValueTypes(ExprValue left, ExprValue right) {
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));
  }

  private static Stream<Arguments> getNotEqualNumericValuesOfDifferentTypes() {
    return Stream.of(
        Arguments.of(new ExprIntegerValue(42), new ExprByteValue(1)),
        Arguments.of(new ExprIntegerValue(42), new ExprShortValue(146)),
        Arguments.of(new ExprIntegerValue(42), new ExprLongValue(100500)),
        Arguments.of(new ExprIntegerValue(42), new ExprFloatValue(-1.5)),
        Arguments.of(new ExprIntegerValue(42), new ExprDoubleValue(1468.84138)));
  }

  @ParameterizedTest
  @MethodSource("getNotEqualNumericValuesOfDifferentTypes")
  public void compareNeqDifferentNumericValueTypes(ExprValue left, ExprValue right) {
    assertNotEquals(0, left.compareTo(right));
    assertNotEquals(0, right.compareTo(left));
  }

  @Test
  public void stringValueCompare() {
    assertEquals(0, new ExprStringValue("str1").compareTo(new ExprStringValue("str1")));
    assertEquals(1, new ExprStringValue("str2").compareTo(new ExprStringValue("str1")));
    assertEquals(-1, new ExprStringValue("str1").compareTo(new ExprStringValue("str2")));
  }

  @Test
  public void intervalValueCompare() {
    assertEquals(
        0,
        new ExprIntervalValue(Period.ofDays(1)).compareTo(new ExprIntervalValue(Period.ofDays(1))));
    assertEquals(
        1,
        new ExprIntervalValue(Period.ofDays(2)).compareTo(new ExprIntervalValue(Period.ofDays(1))));
    assertEquals(
        -1,
        new ExprIntervalValue(Period.ofDays(1)).compareTo(new ExprIntervalValue(Period.ofDays(2))));
  }

  @Test
  public void missingCompareToMethodShouldNotBeenCalledDirectly() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> LITERAL_MISSING.compareTo(LITERAL_FALSE));
    assertEquals(
        "[BUG] Unreachable, Comparing with NULL or MISSING is undefined", exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class, () -> LITERAL_FALSE.compareTo(LITERAL_MISSING));
    assertEquals(
        "[BUG] Unreachable, Comparing with NULL or MISSING is undefined", exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class, () -> ExprMissingValue.of().compare(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with MISSING is undefined", exception.getMessage());
  }

  @Test
  public void nullCompareToMethodShouldNotBeenCalledDirectly() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> LITERAL_NULL.compareTo(LITERAL_FALSE));
    assertEquals(
        "[BUG] Unreachable, Comparing with NULL or MISSING is undefined", exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class, () -> LITERAL_FALSE.compareTo(LITERAL_NULL));
    assertEquals(
        "[BUG] Unreachable, Comparing with NULL or MISSING is undefined", exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class, () -> ExprNullValue.of().compare(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with NULL is undefined", exception.getMessage());
  }
}
