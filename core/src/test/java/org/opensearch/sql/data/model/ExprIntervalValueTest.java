/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;

import java.time.Duration;
import java.time.Period;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprIntervalValueTest {
  @Test
  public void equals_to_self() {
    ExprValue interval = ExprValueUtils.intervalValue(Duration.ofNanos(1000));
    assertEquals(interval.intervalValue(), Duration.ofNanos(1000));
  }

  @Test
  public void equal() {
    ExprValue v1 = new ExprIntervalValue(Duration.ofMinutes(1));
    ExprValue v2 = ExprValueUtils.intervalValue(Duration.ofSeconds(60));
    assertTrue(v1.equals(v2));
  }

  @Test
  public void compare() {
    ExprIntervalValue v1 = new ExprIntervalValue(Period.ofDays(1));
    ExprIntervalValue v2 = new ExprIntervalValue(Period.ofDays(2));
    assertEquals(v1.compare(v2), -1);
  }

  @Test
  public void invalid_compare() {
    ExprIntervalValue v1 = new ExprIntervalValue(Period.ofYears(1));
    ExprIntervalValue v2 = new ExprIntervalValue(Duration.ofHours(1));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> v1.compare(v2),
        String.format("invalid to compare intervals with units %s and %s", v1.unit(), v2.unit()));
  }

  @Test
  public void invalid_get_value() {
    ExprDateValue value = new ExprDateValue("2020-08-20");
    assertThrows(
        ExpressionEvaluationException.class,
        value::intervalValue,
        String.format("invalid to get intervalValue from value of type %s", value.type()));
  }

  @Test
  public void value() {
    ExprValue value = new ExprIntervalValue(Period.ofWeeks(1));
    assertEquals(value.value(), Period.ofWeeks(1));
  }

  @Test
  public void type() {
    ExprValue interval = new ExprIntervalValue(Period.ofYears(1));
    assertEquals(interval.type(), INTERVAL);
  }
}
