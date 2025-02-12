/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;

import java.time.Duration;
import java.time.Period;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

class ExprIntervalValueTest {

  @Test
  void equals_to_self() {
    ExprValue interval = ExprValueUtils.intervalValue(Duration.ofNanos(1000L));
    assertEquals(interval.intervalValue(), Duration.ofNanos(1000L));
  }

  @Test
  void equal() {
    ExprValue v1 = new ExprIntervalValue(Duration.ofMinutes(1L));
    ExprValue v2 = ExprValueUtils.intervalValue(Duration.ofSeconds(60L));
    assertEquals(v1, v2);
  }

  @Test
  void compare() {
    ExprIntervalValue v1 = new ExprIntervalValue(Period.ofDays(1));
    ExprIntervalValue v2 = new ExprIntervalValue(Period.ofDays(2));
    assertEquals(-1, v1.compare(v2));
  }

  @Test
  void invalid_compare() {
    ExprIntervalValue v1 = new ExprIntervalValue(Period.ofYears(1));
    ExprIntervalValue v2 = new ExprIntervalValue(Duration.ofHours(1L));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> v1.compare(v2),
        String.format("invalid to compare intervals with units %s and %s", v1.unit(), v2.unit()));
  }

  @Test
  void invalid_get_value() {
    ExprDateValue value = new ExprDateValue("2020-08-20");
    assertThrows(
        ExpressionEvaluationException.class,
        value::intervalValue,
        String.format("invalid to get intervalValue from value of type %s", value.type()));
  }

  @Test
  void value() {
    ExprValue value = new ExprIntervalValue(Period.ofWeeks(1));
    assertEquals(value.value(), Period.ofWeeks(1));
  }

  @Test
  void type() {
    ExprValue interval = new ExprIntervalValue(Period.ofYears(1));
    assertEquals(INTERVAL, interval.type());
  }

  @Test
  void testHashCode() {
    Duration oneMinute = Duration.ofMinutes(1L);
    Duration sixtySeconds = Duration.ofSeconds(60L);
    Duration twentyFourHours = Duration.ofHours(24L);
    Period oneDay = Period.ofDays(1);
    Period oneMonth = Period.ofMonths(1);

    assertEquals(oneMinute.hashCode(), ExprValueUtils.intervalValue(oneMinute).hashCode());
    assertEquals(oneMinute.hashCode(), ExprValueUtils.intervalValue(sixtySeconds).hashCode());
    assertNotEquals(oneMinute.hashCode(), ExprValueUtils.intervalValue(twentyFourHours).hashCode());
    assertNotEquals(oneMinute.hashCode(), ExprValueUtils.intervalValue(oneMonth).hashCode());

    assertEquals(oneDay.hashCode(), ExprValueUtils.intervalValue(oneDay).hashCode());
    assertNotEquals(oneDay.hashCode(), ExprValueUtils.intervalValue(twentyFourHours).hashCode());
    assertNotEquals(oneDay.hashCode(), ExprValueUtils.intervalValue(oneMonth).hashCode());
  }
}
