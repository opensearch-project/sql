/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.streaming.watermark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BoundedOutOfOrderWatermarkGeneratorTest {

  @Test
  void shouldAdvanceWatermarkIfNewerEvent() {
    assertWatermarkGenerator()
        .thatAllowMaxDelay(100)
        .afterSeenEventTime(1000)
        .shouldGenerateWatermark(899)
        .afterSeenEventTime(2000)
        .shouldGenerateWatermark(1899);
  }

  @Test
  void shouldNotAdvanceWatermarkIfLateEvent() {
    assertWatermarkGenerator()
        .thatAllowMaxDelay(100)
        .afterSeenEventTime(1000)
        .shouldGenerateWatermark(899)
        .afterSeenEventTime(500)
        .shouldGenerateWatermark(899)
        .afterSeenEventTime(999)
        .shouldGenerateWatermark(899);
  }

  private static AssertionHelper assertWatermarkGenerator() {
    return new AssertionHelper();
  }

  private static class AssertionHelper {

    private WatermarkGenerator generator;

    private long actualResult;

    public AssertionHelper thatAllowMaxDelay(long delay) {
      this.generator = new BoundedOutOfOrderWatermarkGenerator(delay);
      return this;
    }

    public AssertionHelper afterSeenEventTime(long timestamp) {
      this.actualResult = generator.generate(timestamp);
      return this;
    }

    public AssertionHelper shouldGenerateWatermark(long expected) {
      assertEquals(expected, actualResult);
      return this;
    }
  }
}
