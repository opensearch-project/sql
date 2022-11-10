/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.expression.DSL.interval;
import static org.opensearch.sql.expression.DSL.literal;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;

class BoundedOutOfOrderWatermarkGeneratorTest {

  @Test
  void shouldAdvanceWatermarkIfNewerEvent() {
    assertNumericWatermarkGenerator()
        .thatAllowMaxDelay(100)
        .afterSeenValue(1000)
        .shouldGenerateWatermark(900)
        .afterSeenValue(2000)
        .shouldGenerateWatermark(1900);

    assertDateTimeWatermarkGenerator()
        .thatAllowMaxDelay(10, "second")
        .afterSeenValue("2022-11-08 00:00:15")
        .shouldGenerateWatermark("2022-11-08 00:00:05")
        .afterSeenValue("2022-11-08 00:00:30")
        .shouldGenerateWatermark("2022-11-08 00:00:20");
  }

  @Test
  void shouldNotAdvanceWatermarkIfLateEvent() {
    assertNumericWatermarkGenerator()
        .thatAllowMaxDelay(100)
        .afterSeenValue(1000)
        .shouldGenerateWatermark(900)
        .afterSeenValue(500)
        .shouldGenerateWatermark(900)
        .afterSeenValue(999)
        .shouldGenerateWatermark(900);

    assertDateTimeWatermarkGenerator()
        .thatAllowMaxDelay(10, "second")
        .afterSeenValue("2022-11-08 00:00:15")
        .shouldGenerateWatermark("2022-11-08 00:00:05")
        .afterSeenValue("2022-11-08 00:00:01")
        .shouldGenerateWatermark("2022-11-08 00:00:05")
        .afterSeenValue("2022-11-08 00:00:10")
        .shouldGenerateWatermark("2022-11-08 00:00:05");
  }

  private static NumericWatermarkAssertionHelper assertNumericWatermarkGenerator() {
    return new NumericWatermarkAssertionHelper();
  }

  private static DateTimeWatermarkAssertionHelper assertDateTimeWatermarkGenerator() {
    return new DateTimeWatermarkAssertionHelper();
  }

  /** Assertion helper for watermark generator handling numeric value. */
  private static class NumericWatermarkAssertionHelper {

    private WatermarkGenerator generator;

    private ExprValue actualResult;

    public NumericWatermarkAssertionHelper thatAllowMaxDelay(int delay) {
      this.generator = new BoundedOutOfOrderWatermarkGenerator(literal(delay));
      return this;
    }

    public NumericWatermarkAssertionHelper afterSeenValue(int value) {
      this.actualResult = generator.generate(integerValue(value));
      return this;
    }

    public NumericWatermarkAssertionHelper shouldGenerateWatermark(int expected) {
      assertEquals(integerValue(expected), actualResult);
      return this;
    }
  }

  /** Assertion helper for watermark generator handling date time value. */
  private static class DateTimeWatermarkAssertionHelper {

    private WatermarkGenerator generator;

    private ExprValue actualResult;

    public DateTimeWatermarkAssertionHelper thatAllowMaxDelay(int delay, String unit) {
      this.generator = new BoundedOutOfOrderWatermarkGenerator(
          interval(literal(delay), literal(unit)));
      return this;
    }

    public DateTimeWatermarkAssertionHelper afterSeenValue(String value) {
      this.actualResult = generator.generate(fromObjectValue(value, DATETIME));
      return this;
    }

    public DateTimeWatermarkAssertionHelper shouldGenerateWatermark(String expected) {
      assertEquals(fromObjectValue(expected, DATETIME), actualResult);
      return this;
    }
  }
}