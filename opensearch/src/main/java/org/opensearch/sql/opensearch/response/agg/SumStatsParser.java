/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.Stats;

/**
 * Parser for SUM aggregation that uses {@link Stats} to correctly return null when all values are
 * null. Per the SQL standard, SUM of all nulls should return null, not 0.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class SumStatsParser implements MetricParser {

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    Stats stats = (Stats) agg;
    Object value = stats.getCount() == 0 ? null : stats.getSum();
    return Collections.singletonList(new HashMap<>(Collections.singletonMap(agg.getName(), value)));
  }
}
