/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** {@link TopHits} metric parser. */
@EqualsAndHashCode
public class TopHitsParser implements MetricParser {

  @Getter private final String name;
  private final boolean returnSingleValue;

  public TopHitsParser(String name) {
    this.name = name;
    this.returnSingleValue = false;
  }

  public TopHitsParser(String name, boolean returnSingleValue) {
    this.name = name;
    this.returnSingleValue = returnSingleValue;
  }

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    if (returnSingleValue) {
      // Extract the single value from the first (and only) hit
      Map<String, Object> source = hits[0].getSourceAsMap();
      if (source.isEmpty()) {
        return Collections.singletonMap(agg.getName(), null);
      }
      // Get the first value from the source map
      Object value = source.values().iterator().next();
      return Collections.singletonMap(agg.getName(), value);
    } else {
      // Return all values as a list
      return Collections.singletonMap(
          agg.getName(),
          Arrays.stream(hits)
              .flatMap(h -> h.getSourceAsMap().values().stream())
              .collect(Collectors.toList()));
    }
  }
}
