/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.metrics.ExtendedStats;

class SumStatsParserTest {

  @Test
  void parse_returnsSum_whenCountPositive() {
    ExtendedStats stats = mock(ExtendedStats.class);
    when(stats.getName()).thenReturn("total");
    when(stats.getCount()).thenReturn(3L);
    when(stats.getSum()).thenReturn(42.0);

    SumStatsParser parser = new SumStatsParser("total");
    List<Map<String, Object>> result = parser.parse(stats);

    assertEquals(1, result.size());
    assertEquals(42.0, result.get(0).get("total"));
  }

  @Test
  void parse_returnsNull_whenCountZero() {
    ExtendedStats stats = mock(ExtendedStats.class);
    when(stats.getName()).thenReturn("total");
    when(stats.getCount()).thenReturn(0L);
    when(stats.getSum()).thenReturn(0.0);

    SumStatsParser parser = new SumStatsParser("total");
    List<Map<String, Object>> result = parser.parse(stats);

    assertEquals(1, result.size());
    assertNull(result.get(0).get("total"));
  }
}
