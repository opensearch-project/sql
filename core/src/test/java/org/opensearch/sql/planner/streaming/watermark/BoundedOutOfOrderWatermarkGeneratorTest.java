/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BoundedOutOfOrderWatermarkGeneratorTest {

  @Test
  void shouldAdvanceWatermarkIfLaterEvent() {
    BoundedOutOfOrderWatermarkGenerator generator = new BoundedOutOfOrderWatermarkGenerator(100);
    assertEquals(899, generator.generate(1000));
    assertEquals(1899, generator.generate(2000));
  }

  @Test
  void shouldNotChangeWatermarkByLateEvent() {
    BoundedOutOfOrderWatermarkGenerator generator = new BoundedOutOfOrderWatermarkGenerator(100);
    assertEquals(899, generator.generate(1000));
    assertEquals(899, generator.generate(500));
    assertEquals(899, generator.generate(700));
  }
}