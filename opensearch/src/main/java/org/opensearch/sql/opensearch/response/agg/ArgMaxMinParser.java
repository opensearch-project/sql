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

/** {@link TopHits} metric parser for ARG_MAX/ARG_MIN aggregations. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class ArgMaxMinParser implements MetricParser {

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    // For ARG_MAX/ARG_MIN, we only need the first (and only) hit
    SearchHit hit = hits[0];
    Map<String, Object> source = hit.getSourceAsMap();

    // Since we configured fetchSource to only include the value field,
    // we should have exactly one field in the source
    if (source.size() == 1) {
      Object value = source.values().iterator().next();
      return Collections.singletonMap(agg.getName(), value);
    } else if (source.isEmpty()) {
      return Collections.singletonMap(agg.getName(), null);
    } else {
      // This shouldn't happen if fetchSource is configured correctly,
      // but as a fallback, return the first value
      Object value = source.values().iterator().next();
      return Collections.singletonMap(agg.getName(), value);
    }
  }
}
