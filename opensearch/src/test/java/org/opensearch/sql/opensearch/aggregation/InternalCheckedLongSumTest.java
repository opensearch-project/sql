/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.search.aggregations.InternalAggregation;

class InternalCheckedLongSumTest {

  @Test
  void reducesExactly() {
    InternalCheckedLongSum result =
        sum(1L << 62, true)
            .reduce(List.<InternalAggregation>of(sum(1L << 62, true), sum(1L, true)), null);

    assertEquals((1L << 62) + 1L, result.longValue());
  }

  @Test
  void throwsOnCrossShardOverflow() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                sum(Long.MAX_VALUE, true)
                    .reduce(
                        List.<InternalAggregation>of(sum(Long.MAX_VALUE, true), sum(1L, true)),
                        null));

    assertEquals(CheckedLongSum.OVERFLOW_MESSAGE, exception.getMessage());
  }

  @Test
  void returnsNullWhenNoShardHasValues() {
    InternalCheckedLongSum result =
        sum(0L, false).reduce(List.<InternalAggregation>of(sum(0L, false), sum(0L, false)), null);

    assertNull(result.longValue());
  }

  @Test
  void serializesExactLongAndNullState() throws IOException {
    InternalCheckedLongSum value = copy(sum((1L << 62) + 1L, true));
    InternalCheckedLongSum empty = copy(sum(0L, false));

    assertEquals((1L << 62) + 1L, value.longValue());
    assertNull(empty.longValue());
  }

  private static InternalCheckedLongSum copy(InternalCheckedLongSum value) throws IOException {
    try (BytesStreamOutput output = new BytesStreamOutput()) {
      value.writeTo(output);
      return new InternalCheckedLongSum(output.bytes().streamInput());
    }
  }

  private static InternalCheckedLongSum sum(long value, boolean hasValue) {
    return new InternalCheckedLongSum("sum", value, hasValue, Map.of());
  }
}
