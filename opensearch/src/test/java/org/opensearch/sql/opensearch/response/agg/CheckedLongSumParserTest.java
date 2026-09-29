/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;

class CheckedLongSumParserTest {

  private final CheckedLongSumParser parser = new CheckedLongSumParser("sum");

  @Test
  void narrowsNativeSumToLong() {
    assertEquals(42L, value(parser.parse(aggregation(42d))));
  }

  @Test
  void preservesNativeDoublePrecisionBehavior() {
    double rounded = (double) ((1L << 62) + 1L);
    assertEquals(1L << 62, value(parser.parse(aggregation(rounded))));
  }

  @Test
  void saturatesAmbiguousPositiveBoundary() {
    assertEquals(Long.MAX_VALUE, value(parser.parse(aggregation((double) Long.MAX_VALUE))));
  }

  @Test
  void rejectsValueClearlyOutsideLongRange() {
    assertThrows(ArithmeticException.class, () -> parser.parse(aggregation(Math.nextUp(0x1p63))));
    assertThrows(
        ArithmeticException.class, () -> parser.parse(aggregation(Math.nextDown(-0x1p63))));
  }

  @Test
  void rejectsInfiniteValue() {
    assertThrows(
        ArithmeticException.class, () -> parser.parse(aggregation(Double.POSITIVE_INFINITY)));
    assertThrows(
        ArithmeticException.class, () -> parser.parse(aggregation(Double.NEGATIVE_INFINITY)));
  }

  @Test
  void convertsNanToNull() {
    assertNull(value(parser.parse(aggregation(Double.NaN))));
  }

  private static NumericMetricsAggregation.SingleValue aggregation(double value) {
    NumericMetricsAggregation.SingleValue aggregation =
        mock(NumericMetricsAggregation.SingleValue.class);
    when(aggregation.getName()).thenReturn("sum");
    when(aggregation.value()).thenReturn(value);
    return aggregation;
  }

  private static Object value(List<Map<String, Object>> rows) {
    return rows.getFirst().get("sum");
  }
}
