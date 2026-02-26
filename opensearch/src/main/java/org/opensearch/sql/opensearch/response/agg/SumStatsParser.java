/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import static org.opensearch.sql.opensearch.response.agg.Utils.handleNanInfValue;

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
 * Parser for SUM aggregation backed by {@link Stats} (extended_stats). Returns null when all values
 * are null/missing (count == 0), matching SQL standard semantics.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class SumStatsParser implements MetricParser {

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    Stats stats = (Stats) agg;
    double value = stats.getCount() == 0 ? Double.NaN : stats.getSum();
    return Collections.singletonList(
        new HashMap<>(Collections.singletonMap(agg.getName(), handleNanInfValue(value))));
  }
}
