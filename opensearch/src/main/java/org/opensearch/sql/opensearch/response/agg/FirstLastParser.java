/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** Parser for FIRST and LAST aggregate functions using TopHits aggregation. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class FirstLastParser implements MetricParser {

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    // Extract the single value from the first (and only) hit
    Map<String, Object> source = hits[0].getSourceAsMap();
    if (source.isEmpty()) {
      return Collections.singletonMap(agg.getName(), null);
    }

    // Get the first value from the source map
    Object value = source.values().iterator().next();
    return Collections.singletonMap(agg.getName(), value);
  }
}
