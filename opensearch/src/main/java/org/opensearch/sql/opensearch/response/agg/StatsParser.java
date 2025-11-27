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
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.ExtendedStats;

/** {@link ExtendedStats} metric parser. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class StatsParser implements MetricParser {

  private final Function<ExtendedStats, Double> valueExtractor;

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    return Collections.singletonList(
        new HashMap<>(
            Collections.singletonMap(
                agg.getName(), handleNanInfValue(valueExtractor.apply((ExtendedStats) agg)))));
  }
}
